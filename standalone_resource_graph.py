#!/usr/bin/env python3
"""
Standalone script to query Azure Resource Graph.

This script replicates the functionality of the on_resync_resource_graph handler
without depending on the Ocean framework.

Usage:
    python standalone_resource_graph.py --query "Resources | project id, name, type"

Environment Variables:
    AZURE_TENANT_ID: Azure tenant ID
    AZURE_CLIENT_ID: Azure client ID (service principal)
    AZURE_CLIENT_SECRET: Azure client secret
    AZURE_BASE_URL: Azure management API base URL (default: https://management.azure.com)

Examples:
    # Basic query
    export AZURE_TENANT_ID="your-tenant-id"
    export AZURE_CLIENT_ID="your-client-id"
    export AZURE_CLIENT_SECRET="your-client-secret"
    python standalone_resource_graph.py --query "Resources | where type == 'microsoft.compute/virtualmachines'"

    # Query with duplicate detection
    python standalone_resource_graph.py -q "Resources | project id, name, type" --check-duplicates

    # Query with custom ID field for duplicate detection
    python standalone_resource_graph.py -q "Resources" --check-duplicates --id-field "id"

    # Save results and duplicate report to file
    python standalone_resource_graph.py -q "Resources" -d -o results.json

Exit Codes:
    0 - Success
    1 - Error (credentials missing, API error, etc.)
    2 - Success but duplicates were found (when --check-duplicates is enabled)
"""

import argparse
import asyncio
import json
import os
import sys
import textwrap
import time
from contextlib import asynccontextmanager
from itertools import batched
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
import httpx

# Load environment variables from .env file
load_dotenv()
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import ClientSecretCredential
from loguru import logger

# ============================================================================
# Configuration
# ============================================================================

DEFAULT_BASE_URL = "https://management.azure.com"
DEFAULT_API_VERSION = "2024-04-01"
DEFAULT_SUBSCRIPTION_API_VERSION = "2022-12-01"
DEFAULT_HTTP_TIMEOUT = 60
DEFAULT_PAGE_SIZE = 100

# Rate limiter defaults (Azure token bucket algorithm)
RATE_LIMIT_CAPACITY = 250
RATE_LIMIT_REFILL_RATE = 25.0

# Batch sizes for processing
SUBSCRIPTION_BATCH_SIZE = 10
MAX_CONCURRENT_REQUESTS = 10


# ============================================================================
# Utilities
# ============================================================================


def format_query(query: str) -> str:
    """Format and normalize a Resource Graph query."""
    query = query.strip().strip("'").strip('"')
    query = textwrap.dedent(query).strip()
    return query


def parse_url_components(url: str) -> tuple[str, dict[str, str]]:
    """Extract endpoint and query params from a URL."""
    parsed = urlparse(url)
    endpoint = parsed.path.rstrip("/")
    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    return endpoint, params


# ============================================================================
# Duplicate Tracking
# ============================================================================


@dataclass
class ResourceOccurrence:
    """Tracks where a resource was seen."""
    id: str
    subscription_batch: int
    page: int
    position_in_page: int
    global_position: int

    def __str__(self) -> str:
        return (
            f"subscription_batch={self.subscription_batch}, "
            f"page={self.page}, position_in_page={self.position_in_page}, "
            f"global_position={self.global_position}"
        )


@dataclass
class DuplicateInfo:
    """Information about a duplicate ID."""
    id: str
    first_occurrence: ResourceOccurrence
    duplicate_occurrences: List[ResourceOccurrence] = field(default_factory=list)


class DuplicateTracker:
    """
    Tracks resource IDs and detects duplicates.
    
    Records the page, position, and subscription batch for each occurrence
    to help diagnose data inconsistency issues.
    """

    def __init__(self, id_field: str = "id"):
        self.id_field = id_field
        self._seen: Dict[str, ResourceOccurrence] = {}
        self._duplicates: Dict[str, DuplicateInfo] = {}
        self._global_position = 0

    def track_batch(
        self,
        resources: List[Dict[str, Any]],
        subscription_batch: int,
        page: int,
    ) -> List[Dict[str, Any]]:
        """
        Track a batch of resources and return any duplicates found in this batch.
        
        Args:
            resources: List of resource dictionaries
            subscription_batch: The subscription batch number (1-indexed)
            page: The page number within the subscription batch (1-indexed)
            
        Returns:
            List of duplicate resources found in this batch
        """
        duplicates_in_batch = []

        for position, resource in enumerate(resources, start=1):
            resource_id = resource.get(self.id_field)
            if resource_id is None:
                logger.warning(
                    f"Resource missing '{self.id_field}' field at "
                    f"subscription_batch={subscription_batch}, page={page}, position={position}"
                )
                continue

            self._global_position += 1
            occurrence = ResourceOccurrence(
                id=resource_id,
                subscription_batch=subscription_batch,
                page=page,
                position_in_page=position,
                global_position=self._global_position,
            )

            if resource_id in self._seen:
                # This is a duplicate
                duplicates_in_batch.append(resource)

                if resource_id not in self._duplicates:
                    self._duplicates[resource_id] = DuplicateInfo(
                        id=resource_id,
                        first_occurrence=self._seen[resource_id],
                    )
                self._duplicates[resource_id].duplicate_occurrences.append(occurrence)

                logger.warning(
                    f"DUPLICATE DETECTED: id='{resource_id}' | "
                    f"First seen: [{self._seen[resource_id]}] | "
                    f"Duplicate at: [{occurrence}]"
                )
            else:
                self._seen[resource_id] = occurrence

        return duplicates_in_batch

    @property
    def total_resources(self) -> int:
        """Total number of resources tracked (including duplicates)."""
        return self._global_position

    @property
    def unique_resources(self) -> int:
        """Number of unique resource IDs."""
        return len(self._seen)

    @property
    def duplicate_count(self) -> int:
        """Number of duplicate IDs found."""
        return len(self._duplicates)

    @property
    def total_duplicate_occurrences(self) -> int:
        """Total number of duplicate occurrences (a single ID can have multiple duplicates)."""
        return sum(len(d.duplicate_occurrences) for d in self._duplicates.values())

    def get_duplicates(self) -> Dict[str, DuplicateInfo]:
        """Get all duplicate information."""
        return self._duplicates

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of duplicate tracking results."""
        return {
            "total_resources_processed": self.total_resources,
            "unique_resources": self.unique_resources,
            "duplicate_ids_count": self.duplicate_count,
            "total_duplicate_occurrences": self.total_duplicate_occurrences,
            "duplicates": {
                id: {
                    "first_occurrence": {
                        "subscription_batch": info.first_occurrence.subscription_batch,
                        "page": info.first_occurrence.page,
                        "position_in_page": info.first_occurrence.position_in_page,
                        "global_position": info.first_occurrence.global_position,
                    },
                    "duplicate_occurrences": [
                        {
                            "subscription_batch": occ.subscription_batch,
                            "page": occ.page,
                            "position_in_page": occ.position_in_page,
                            "global_position": occ.global_position,
                        }
                        for occ in info.duplicate_occurrences
                    ],
                    "total_occurrences": 1 + len(info.duplicate_occurrences),
                }
                for id, info in self._duplicates.items()
            },
        }

    def print_report(self) -> None:
        """Print a human-readable duplicate report."""
        print("\n" + "=" * 80)
        print("DUPLICATE DETECTION REPORT")
        print("=" * 80)
        print(f"Total resources processed: {self.total_resources}")
        print(f"Unique resource IDs: {self.unique_resources}")
        print(f"Duplicate IDs found: {self.duplicate_count}")
        print(f"Total duplicate occurrences: {self.total_duplicate_occurrences}")

        if self._duplicates:
            print("\n" + "-" * 80)
            print("DUPLICATE DETAILS:")
            print("-" * 80)

            for resource_id, info in self._duplicates.items():
                print(f"\nID: {resource_id}")
                print(f"  Total occurrences: {1 + len(info.duplicate_occurrences)}")
                print(f"  First occurrence:")
                print(f"    - Subscription batch: {info.first_occurrence.subscription_batch}")
                print(f"    - Page: {info.first_occurrence.page}")
                print(f"    - Position in page: {info.first_occurrence.position_in_page}")
                print(f"    - Global position: {info.first_occurrence.global_position}")
                print(f"  Duplicate occurrences ({len(info.duplicate_occurrences)}):")
                for i, occ in enumerate(info.duplicate_occurrences, start=1):
                    print(f"    [{i}] Subscription batch: {occ.subscription_batch}, "
                          f"Page: {occ.page}, Position: {occ.position_in_page}, "
                          f"Global: {occ.global_position}")
        else:
            print("\nNo duplicates found!")

        print("=" * 80 + "\n")


# ============================================================================
# Rate Limiter
# ============================================================================


class AdaptiveRateLimiter:
    """
    Adaptive Token Bucket Rate Limiter compatible with Azure's throttling.
    
    Based on Azure's token bucket algorithm:
    https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/request-limits-and-throttling
    """

    def __init__(
        self,
        capacity: int = RATE_LIMIT_CAPACITY,
        refill_rate: float = RATE_LIMIT_REFILL_RATE,
        max_wait: int = 30,
    ) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._adaptive_refill_rate = refill_rate
        self.max_wait = max_wait
        self.tokens = float(capacity)
        self.last_refill_time = time.monotonic()
        self._lock = asyncio.Lock()
        self._last_adjustment_time = 0.0

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        added_tokens = elapsed * self._adaptive_refill_rate
        if added_tokens > 0:
            self.tokens = min(self.capacity, self.tokens + added_tokens)
            self.last_refill_time = now

    async def _consume(self, tokens: float = 1.0) -> None:
        async with self._lock:
            while True:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                needed = tokens - self.tokens
                wait_time = needed / max(self._adaptive_refill_rate, 0.01)
                if wait_time > self.max_wait:
                    raise TimeoutError("Rate limiter wait exceeded max_wait.")

                logger.warning(f"Waiting {wait_time:.2f}s for token refill")
                await asyncio.sleep(wait_time)

    @asynccontextmanager
    async def limit(self, tokens: float = 1.0) -> AsyncGenerator[None, None]:
        await self._consume(tokens)
        yield

    def adjust_from_headers(self, headers: dict[str, str]) -> None:
        """Adjust refill rate based on Azure API quota headers."""
        now = time.monotonic()
        if now - self._last_adjustment_time < 1.0:
            return
        self._last_adjustment_time = now

        if "x-ms-ratelimit-remaining-tenant-reads" in headers:
            try:
                quota_remaining = int(headers["x-ms-ratelimit-remaining-tenant-reads"])
                remaining_ratio = quota_remaining / float(self.capacity)

                if remaining_ratio < 0.1:
                    slowdown_factor = max(0.1, remaining_ratio)
                    self._adaptive_refill_rate = self.refill_rate * slowdown_factor
                    logger.warning(
                        f"Nearing quota exhaustion (remaining={quota_remaining}), "
                        f"reducing refill rate to {self._adaptive_refill_rate:.2f}/s"
                    )
                elif remaining_ratio > 0.8 and self._adaptive_refill_rate < self.refill_rate:
                    self._adaptive_refill_rate = min(
                        self.refill_rate, self._adaptive_refill_rate * 1.15
                    )
                    logger.info(f"Recovering refill rate to {self._adaptive_refill_rate:.2f}/s")
            except (ValueError, KeyError):
                pass


# ============================================================================
# Azure REST Client
# ============================================================================


class AzureRestClient:
    """Base async Azure REST client with auth, rate limiting, and error handling."""

    def __init__(
        self,
        credential: AsyncTokenCredential,
        base_url: str,
        rate_limiter: AdaptiveRateLimiter,
    ) -> None:
        self.credential = credential
        self.base_url = base_url
        self.rate_limiter = rate_limiter
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=DEFAULT_HTTP_TIMEOUT)
        return self._client

    @property
    def scope(self) -> str:
        return self.base_url + "/.default"

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    async def get_headers(self) -> Dict[str, str]:
        token = (await self.credential.get_token(self.scope)).token
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a request to Azure API with rate limiting."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = params or {}
        json_body = json_body or {}

        logger.debug(f"Making {method} request to {endpoint}")

        async with self.rate_limiter.limit():
            try:
                headers = await self.get_headers()
                response = await self.client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body if json_body else None,
                    headers=headers,
                )
                response.raise_for_status()
                self.rate_limiter.adjust_from_headers(dict(response.headers))
                return response.json()

            except httpx.HTTPStatusError as e:
                if e.response.status_code in (401, 403, 404):
                    logger.warning(
                        f"Request to {url} failed with status {e.response.status_code}: {e.response.text}"
                    )
                    return {}
                logger.error(
                    f"Azure API error for '{url}': Status {e.response.status_code}, Response: {e.response.text}"
                )
                raise

            except httpx.HTTPError as e:
                logger.error(f"Network error for endpoint '{url}': {str(e)}")
                raise


# ============================================================================
# Subscription Client
# ============================================================================


class SubscriptionClient:
    """Client for fetching Azure subscriptions."""

    def __init__(self, rest_client: AzureRestClient, api_version: str = DEFAULT_SUBSCRIPTION_API_VERSION):
        self.rest_client = rest_client
        self.api_version = api_version

    async def get_subscriptions(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Fetch all subscriptions with pagination."""
        next_url = "subscriptions"
        params = {"api-version": self.api_version}

        while next_url:
            response = await self.rest_client.make_request(
                method="GET",
                endpoint=next_url,
                params=params,
            )
            if not response:
                break

            subscriptions = response.get("value", [])
            logger.info(f"Fetched batch of {len(subscriptions)} subscriptions")

            for batch in batched(subscriptions, DEFAULT_PAGE_SIZE):
                yield list(batch)

            next_link = response.get("nextLink")
            if not next_link:
                break
            next_url, params = parse_url_components(next_link)
            if "api-version" not in params:
                params["api-version"] = self.api_version


# ============================================================================
# Resource Graph Client
# ============================================================================


class ResourceGraphClient:
    """Client for querying Azure Resource Graph."""

    def __init__(self, rest_client: AzureRestClient, api_version: str = DEFAULT_API_VERSION):
        self.rest_client = rest_client
        self.api_version = api_version

    async def query(
        self,
        query: str,
        subscription_ids: List[str],
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Execute a Resource Graph query with pagination."""
        formatted_query = format_query(query)
        skip_token = None
        page = 1
        total_fetched = 0

        while True:
            json_body = {
                "query": formatted_query,
                "subscriptions": subscription_ids,
                "options": {"$skipToken": skip_token},
            }
            params = {"api-version": self.api_version}

            response = await self.rest_client.make_request(
                method="POST",
                endpoint="providers/Microsoft.ResourceGraph/resources",
                params=params,
                json_body=json_body,
            )

            if not response:
                break

            skip_token = response.get("$skipToken")
            data = response.get("data", [])
            total_records = response.get("totalRecords", 0)
            count = response.get("count", len(data))

            logger.info(
                f"Retrieved batch of {count} out of {total_records} total records from page {page}"
            )
            total_fetched += count

            for batch in batched(data, DEFAULT_PAGE_SIZE):
                yield list(batch)

            if not skip_token:
                break
            page += 1

        logger.info(f"Retrieved all {total_fetched} records from Resource Graph")


# ============================================================================
# Main Orchestrator
# ============================================================================


async def stream_async_iterators(*iterators: AsyncGenerator) -> AsyncGenerator[Any, None]:
    """Stream results from multiple async generators concurrently."""
    tasks = [asyncio.create_task(it.__anext__()) for it in iterators]
    iterator_map = {id(task): (i, it) for i, (task, it) in enumerate(zip(tasks, iterators))}

    while tasks:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            idx, iterator = iterator_map.pop(id(task))
            tasks.remove(task)

            try:
                result = task.result()
                yield result
                # Schedule next item from same iterator
                new_task = asyncio.create_task(iterator.__anext__())
                tasks.append(new_task)
                iterator_map[id(new_task)] = (idx, iterator)
            except StopAsyncIteration:
                pass


async def stream_async_iterators_with_index(
    *iterators: AsyncGenerator,
) -> AsyncGenerator[tuple[int, Any], None]:
    """
    Stream results from multiple async generators concurrently, yielding (index, result) tuples.
    
    The index indicates which iterator produced the result.
    """
    tasks = [asyncio.create_task(it.__anext__()) for it in iterators]
    iterator_map = {id(task): (i, it) for i, (task, it) in enumerate(zip(tasks, iterators))}

    while tasks:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            idx, iterator = iterator_map.pop(id(task))
            tasks.remove(task)

            try:
                result = task.result()
                yield idx, result
                # Schedule next item from same iterator
                new_task = asyncio.create_task(iterator.__anext__())
                tasks.append(new_task)
                iterator_map[id(new_task)] = (idx, iterator)
            except StopAsyncIteration:
                pass


@dataclass
class QueryResult:
    """Result of a Resource Graph query with duplicate tracking info."""
    resources: List[Dict[str, Any]]
    duplicate_tracker: Optional[DuplicateTracker] = None

    @property
    def has_duplicates(self) -> bool:
        return self.duplicate_tracker is not None and self.duplicate_tracker.duplicate_count > 0


async def query_resource_graph(
    query: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    base_url: str = DEFAULT_BASE_URL,
    api_version: str = DEFAULT_API_VERSION,
    subscription_api_version: str = DEFAULT_SUBSCRIPTION_API_VERSION,
    output_file: Optional[str] = None,
    check_duplicates: bool = False,
    id_field: str = "id",
) -> QueryResult:
    """
    Query Azure Resource Graph across all accessible subscriptions.
    
    This function replicates the on_resync_resource_graph handler logic.
    
    Args:
        query: KQL query to execute
        tenant_id: Azure tenant ID
        client_id: Azure client ID
        client_secret: Azure client secret
        base_url: Azure management API base URL
        api_version: Resource Graph API version
        subscription_api_version: Subscription API version
        output_file: Optional file path to save results
        check_duplicates: Enable duplicate ID detection
        id_field: Field name to use as unique identifier (default: "id")
        
    Returns:
        QueryResult containing resources and optional duplicate tracking info
    """
    # Create Azure credential
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    # Create rate limiter and clients
    rate_limiter = AdaptiveRateLimiter()
    rest_client = AzureRestClient(credential, base_url, rate_limiter)
    subscription_client = SubscriptionClient(rest_client, subscription_api_version)
    resource_graph_client = ResourceGraphClient(rest_client, api_version)

    all_results: List[Dict[str, Any]] = []
    duplicate_tracker = DuplicateTracker(id_field=id_field) if check_duplicates else None

    # Track subscription batch and page numbers for duplicate detection
    subscription_batch_num = 0
    page_num = 0

    try:
        # Fetch subscriptions and query Resource Graph
        async for subscriptions in subscription_client.get_subscriptions():
            logger.info(f"Processing {len(subscriptions)} subscriptions")

            # Process subscriptions in batches
            tasks = []
            task_batch_nums = []  # Track which subscription batch each task belongs to

            for subscription_batch in batched(subscriptions, SUBSCRIPTION_BATCH_SIZE):
                subscription_batch_num += 1
                subscription_ids = [sub["subscriptionId"] for sub in subscription_batch]
                logger.info(
                    f"Querying Resource Graph for subscription batch {subscription_batch_num} "
                    f"({len(subscription_ids)} subscriptions)"
                )

                tasks.append(resource_graph_client.query(query, subscription_ids))
                task_batch_nums.append(subscription_batch_num)

                # Process tasks when we reach max concurrent requests
                if len(tasks) >= MAX_CONCURRENT_REQUESTS:
                    task_page_counters = {i: 0 for i in range(len(tasks))}
                    async for idx, results in stream_async_iterators_with_index(*tasks):
                        task_page_counters[idx] += 1
                        page_num = task_page_counters[idx]
                        batch_num = task_batch_nums[idx]

                        logger.info(
                            f"Received batch of {len(results)} results from "
                            f"subscription_batch={batch_num}, page={page_num}"
                        )

                        if duplicate_tracker:
                            duplicate_tracker.track_batch(results, batch_num, page_num)

                        all_results.extend(results)
                    tasks.clear()
                    task_batch_nums.clear()

            # Process remaining tasks
            if tasks:
                task_page_counters = {i: 0 for i in range(len(tasks))}
                async for idx, results in stream_async_iterators_with_index(*tasks):
                    task_page_counters[idx] += 1
                    page_num = task_page_counters[idx]
                    batch_num = task_batch_nums[idx]

                    logger.info(
                        f"Received batch of {len(results)} results from "
                        f"subscription_batch={batch_num}, page={page_num}"
                    )

                    if duplicate_tracker:
                        duplicate_tracker.track_batch(results, batch_num, page_num)

                    all_results.extend(results)

    finally:
        await rest_client.close()
        await credential.close()

    logger.info(f"Total results fetched: {len(all_results)}")

    if duplicate_tracker:
        logger.info(
            f"Duplicate check: {duplicate_tracker.unique_resources} unique, "
            f"{duplicate_tracker.duplicate_count} duplicate IDs, "
            f"{duplicate_tracker.total_duplicate_occurrences} total duplicate occurrences"
        )

    # Optionally save to file
    if output_file:
        output_data = {
            "resources": all_results,
            "metadata": {
                "total_count": len(all_results),
                "query": query,
            },
        }
        if duplicate_tracker:
            output_data["duplicate_report"] = duplicate_tracker.get_summary()

        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Results saved to {output_file}")

    return QueryResult(resources=all_results, duplicate_tracker=duplicate_tracker)


# ============================================================================
# CLI
# ============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Azure Resource Graph across all subscriptions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--query", "-q",
        required=True,
        help="Resource Graph query (KQL)",
    )
    parser.add_argument(
        "--tenant-id",
        default=os.environ.get("AZURE_TENANT_ID"),
        help="Azure tenant ID (or set AZURE_TENANT_ID env var)",
    )
    parser.add_argument(
        "--client-id",
        default=os.environ.get("AZURE_CLIENT_ID"),
        help="Azure client ID (or set AZURE_CLIENT_ID env var)",
    )
    parser.add_argument(
        "--client-secret",
        default=os.environ.get("AZURE_CLIENT_SECRET"),
        help="Azure client secret (or set AZURE_CLIENT_SECRET env var)",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("AZURE_BASE_URL", DEFAULT_BASE_URL),
        help=f"Azure management API base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--api-version",
        default=DEFAULT_API_VERSION,
        help=f"Resource Graph API version (default: {DEFAULT_API_VERSION})",
    )
    parser.add_argument(
        "--subscription-api-version",
        default=DEFAULT_SUBSCRIPTION_API_VERSION,
        help=f"Subscription API version (default: {DEFAULT_SUBSCRIPTION_API_VERSION})",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path (JSON format)",
    )
    parser.add_argument(
        "--check-duplicates", "-d",
        action="store_true",
        help="Enable duplicate ID detection and report",
    )
    parser.add_argument(
        "--id-field",
        default="id",
        help="Field name to use as unique identifier for duplicate detection (default: 'id')",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress all logs except errors",
    )
    parser.add_argument(
        "--summary", "-s",
        action="store_true",
        help="Show only summary (count) without printing results",
    )
    return parser.parse_args()


def configure_logging(verbose: bool = False, quiet: bool = False) -> None:
    logger.remove()
    if quiet:
        logger.add(sys.stderr, level="ERROR")
    elif verbose:
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.add(sys.stderr, level="INFO")


async def main() -> int:
    args = parse_args()
    configure_logging(args.verbose, args.quiet)

    # Validate credentials
    if not all([args.tenant_id, args.client_id, args.client_secret]):
        logger.error(
            "Missing Azure credentials. Provide --tenant-id, --client-id, --client-secret "
            "or set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET environment variables."
        )
        return 1

    try:
        result = await query_resource_graph(
            query=args.query,
            tenant_id=args.tenant_id,
            client_id=args.client_id,
            client_secret=args.client_secret,
            base_url=args.base_url,
            api_version=args.api_version,
            subscription_api_version=args.subscription_api_version,
            output_file=args.output,
            check_duplicates=args.check_duplicates,
            id_field=args.id_field,
        )

        # Print duplicate report if enabled
        if args.check_duplicates and result.duplicate_tracker:
            result.duplicate_tracker.print_report()

            if result.has_duplicates:
                logger.warning(
                    f"Found {result.duplicate_tracker.duplicate_count} duplicate IDs! "
                    "See report above for details."
                )

        # Print results to stdout if no output file specified
        if not args.output:
            if args.summary:
                print(f"\nTotal resources: {len(result.resources)}")
            else:
                print(json.dumps(result.resources, indent=2))

        # Return non-zero exit code if duplicates found (useful for CI/CD)
        if args.check_duplicates and result.has_duplicates:
            return 2

        return 0

    except Exception as e:
        logger.exception(f"Error querying Resource Graph: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
