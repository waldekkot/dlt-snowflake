-- GitHub Tutorial Stored Procedures
-- Database: DLT_SNOWPARK_SFRT
-- Schema: GITHUB_TUTORIAL
-- These procedures demonstrate dlt patterns with GitHub API
-- Based on: https://dlthub.com/docs/tutorial/load-data-from-an-api

-- Prerequisites (run once):
-- CREATE NETWORK RULE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.GITHUB_API_NETWORK_RULE
--   MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('api.github.com:443');
-- CREATE EXTERNAL ACCESS INTEGRATION GITHUB_API_ACCESS_INTEGRATION
--   ALLOWED_NETWORK_RULES = (DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.GITHUB_API_NETWORK_RULE)
--   ENABLED = TRUE;

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL."P_GITHUB_ISSUES_BASIC"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Basic GitHub Issues Example

    Equivalent to the tutorial''s first example:
    - Fetches issues from GitHub API
    - Loads them into Snowflake using dlt
    """
    import dlt
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="github_issues_basic")

    # Fetch issues from GitHub API
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"
    response = requests.get(url, params={"per_page": 30, "state": "open"})
    response.raise_for_status()
    issues_data = response.json()

    @dlt.resource(name="issues", write_disposition="replace")
    def get_issues():
        # Flatten complex nested objects for better table structure
        flattened = []
        for issue in issues_data:
            flat_issue = {
                "id": issue.get("id"),
                "number": issue.get("number"),
                "title": issue.get("title"),
                "state": issue.get("state"),
                "created_at": issue.get("created_at"),
                "updated_at": issue.get("updated_at"),
                "user_login": issue.get("user", {}).get("login") if issue.get("user") else None,
                "user_id": issue.get("user", {}).get("id") if issue.get("user") else None,
                "comments": issue.get("comments"),
                "body": issue.get("body", "")[:500] if issue.get("body") else None,  # Truncate long bodies
                "html_url": issue.get("html_url"),
                "labels": json.dumps([l.get("name") for l in issue.get("labels", [])]),
            }
            flattened.append(flat_issue)
        yield flattened

    result = loader.run(get_issues())
    verification = loader.verify_table("issues")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "source": "https://api.github.com/repos/dlt-hub/dlt/issues"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL."P_GITHUB_ISSUES_REPLACE"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Replace Mode Example

    Demonstrates idempotent loading - run multiple times,
    table always contains exactly one copy of the data.
    """
    import dlt
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="github_issues_replace")

    # Fetch closed issues (different from open)
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"
    response = requests.get(url, params={
        "per_page": 20,
        "state": "closed",
        "sort": "updated",
        "direction": "desc"
    })
    response.raise_for_status()
    issues_data = response.json()

    @dlt.resource(name="closed_issues", write_disposition="replace")
    def get_closed_issues():
        flattened = []
        for issue in issues_data:
            flat_issue = {
                "id": issue.get("id"),
                "number": issue.get("number"),
                "title": issue.get("title"),
                "state": issue.get("state"),
                "closed_at": issue.get("closed_at"),
                "created_at": issue.get("created_at"),
                "updated_at": issue.get("updated_at"),
                "user_login": issue.get("user", {}).get("login") if issue.get("user") else None,
                "comments": issue.get("comments"),
            }
            flattened.append(flat_issue)
        yield flattened

    result = loader.run(get_closed_issues())
    verification = loader.verify_table("closed_issues")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "note": "Run this procedure multiple times - table will always have same row count"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL."P_GITHUB_ISSUES_PAGINATED"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Pagination Example using dlt''s REST client

    Uses dlt.sources.helpers.rest_client.paginate() to handle
    GitHub API pagination automatically.
    """
    import dlt
    import json
    from dlt_snowpark import DltSnowparkLoader
    from dlt.sources.helpers.rest_client import paginate

    loader = DltSnowparkLoader(session, pipeline_name="github_issues_paginated")

    @dlt.resource(name="issues_paginated", write_disposition="replace")
    def get_issues():
        """Fetch issues with automatic pagination"""
        page_count = 0
        for page in paginate(
            "https://api.github.com/repos/dlt-hub/dlt/issues",
            params={
                "per_page": 50,
                "state": "all",
                "sort": "created",
                "direction": "desc"
            }
        ):
            page_count += 1
            # Flatten each issue in the page
            flattened_page = []
            for issue in page:
                flat_issue = {
                    "id": issue.get("id"),
                    "number": issue.get("number"),
                    "title": issue.get("title"),
                    "state": issue.get("state"),
                    "created_at": issue.get("created_at"),
                    "updated_at": issue.get("updated_at"),
                    "closed_at": issue.get("closed_at"),
                    "user_login": issue.get("user", {}).get("login") if issue.get("user") else None,
                    "comments": issue.get("comments"),
                    "html_url": issue.get("html_url"),
                }
                flattened_page.append(flat_issue)
            yield flattened_page

            # Limit to 3 pages for demo (avoid rate limiting)
            if page_count >= 3:
                break

    result = loader.run(get_issues())
    verification = loader.verify_table("issues_paginated")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "note": "Used dlt paginate() helper for automatic pagination"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL."P_GITHUB_SOURCE_MULTI_TABLE"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Multiple Resources Example (Source Pattern)

    Loads both issues and comments in a single pipeline run,
    similar to the @dlt.source pattern in the tutorial.
    """
    import dlt
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="github_source")

    # Fetch issues
    issues_response = requests.get(
        "https://api.github.com/repos/dlt-hub/dlt/issues",
        params={"per_page": 20, "state": "open"}
    )
    issues_response.raise_for_status()
    issues_data = issues_response.json()

    # Fetch comments (repo-level comments endpoint)
    comments_response = requests.get(
        "https://api.github.com/repos/dlt-hub/dlt/issues/comments",
        params={"per_page": 30, "sort": "created", "direction": "desc"}
    )
    comments_response.raise_for_status()
    comments_data = comments_response.json()

    @dlt.resource(name="gh_issues", write_disposition="replace")
    def get_issues():
        flattened = []
        for issue in issues_data:
            flat_issue = {
                "id": issue.get("id"),
                "number": issue.get("number"),
                "title": issue.get("title"),
                "state": issue.get("state"),
                "created_at": issue.get("created_at"),
                "updated_at": issue.get("updated_at"),
                "user_login": issue.get("user", {}).get("login") if issue.get("user") else None,
                "comments_count": issue.get("comments"),
            }
            flattened.append(flat_issue)
        yield flattened

    @dlt.resource(name="gh_comments", write_disposition="replace")
    def get_comments():
        flattened = []
        for comment in comments_data:
            flat_comment = {
                "id": comment.get("id"),
                "issue_url": comment.get("issue_url"),
                "body": comment.get("body", "")[:500] if comment.get("body") else None,
                "created_at": comment.get("created_at"),
                "updated_at": comment.get("updated_at"),
                "user_login": comment.get("user", {}).get("login") if comment.get("user") else None,
                "user_id": comment.get("user", {}).get("id") if comment.get("user") else None,
            }
            flattened.append(flat_comment)
        yield flattened

    # Load both resources in single pipeline run
    result = loader.run(get_issues(), get_comments())

    # Verify both tables
    issues_verification = loader.verify_table("gh_issues")
    comments_verification = loader.verify_table("gh_comments")

    return json.dumps({
        "load_result": result,
        "verifications": {
            "issues": issues_verification,
            "comments": comments_verification
        },
        "note": "Loaded issues and comments in single pipeline run (source pattern)"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL."P_GITHUB_DYNAMIC_RESOURCES"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Dynamic Resources Example

    Creates resources dynamically for multiple API endpoints,
    reducing code duplication. Similar to the tutorial''s
    fetch_github_data() pattern.
    """
    import json
    import dlt
    from dlt_snowpark import DltSnowparkLoader
    from dlt.sources.helpers.rest_client import paginate

    loader = DltSnowparkLoader(session, pipeline_name="github_dynamic")

    BASE_URL = "https://api.github.com/repos/dlt-hub/dlt"

    # Define endpoints to fetch
    ENDPOINTS = [
        {"name": "issues", "path": "/issues", "params": {"per_page": 20, "state": "open"}},
        {"name": "pulls", "path": "/pulls", "params": {"per_page": 20, "state": "open"}},
        {"name": "releases", "path": "/releases", "params": {"per_page": 10}},
    ]

    def flatten_item(item, endpoint_name):
        """Generic flattener for GitHub API responses"""
        base = {
            "id": item.get("id"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        }

        if endpoint_name == "issues":
            base.update({
                "number": item.get("number"),
                "title": item.get("title"),
                "state": item.get("state"),
                "user_login": item.get("user", {}).get("login") if item.get("user") else None,
            })
        elif endpoint_name == "pulls":
            base.update({
                "number": item.get("number"),
                "title": item.get("title"),
                "state": item.get("state"),
                "user_login": item.get("user", {}).get("login") if item.get("user") else None,
                "merged_at": item.get("merged_at"),
                "draft": item.get("draft"),
            })
        elif endpoint_name == "releases":
            base.update({
                "tag_name": item.get("tag_name"),
                "name": item.get("name"),
                "draft": item.get("draft"),
                "prerelease": item.get("prerelease"),
                "published_at": item.get("published_at"),
                "author_login": item.get("author", {}).get("login") if item.get("author") else None,
            })

        return base

    def fetch_endpoint(endpoint_config):
        """Generator that fetches data from an endpoint"""
        url = BASE_URL + endpoint_config["path"]
        for page in paginate(url, params=endpoint_config["params"]):
            flattened = [flatten_item(item, endpoint_config["name"]) for item in page]
            yield flattened
            break  # Only first page for demo

    # Create resources dynamically
    resources = []
    for endpoint in ENDPOINTS:
        resource = dlt.resource(
            fetch_endpoint(endpoint),
            name=f"dyn_{endpoint[''name'']}",
            write_disposition="replace",
        )
        resources.append(resource)

    # Run all resources
    result = loader.run(*resources)

    # Verify all tables
    verifications = {}
    for endpoint in ENDPOINTS:
        table_name = f"dyn_{endpoint[''name'']}"
        verifications[table_name] = loader.verify_table(table_name)

    return json.dumps({
        "load_result": result,
        "verifications": verifications,
        "note": "Created resources dynamically for multiple endpoints"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL."P_GITHUB_CONFIGURABLE"("REPO_NAME" VARCHAR DEFAULT 'dlt-hub/dlt')
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, repo_name: str):
    """
    Configurable Source Example

    Accepts repo_name as parameter to load data from any GitHub repo.
    Similar to the tutorial''s dlt.config.value pattern.

    Args:
        repo_name: GitHub repo in format "owner/repo" (default: "dlt-hub/dlt")
    """
    import dlt
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="github_configurable")

    base_url = f"https://api.github.com/repos/{repo_name}"

    # Fetch repo info
    repo_response = requests.get(base_url)
    repo_response.raise_for_status()
    repo_data = repo_response.json()

    # Fetch issues
    issues_response = requests.get(
        f"{base_url}/issues",
        params={"per_page": 10, "state": "open"}
    )
    issues_response.raise_for_status()
    issues_data = issues_response.json()

    @dlt.resource(name="cfg_repo_info", write_disposition="replace")
    def get_repo_info():
        yield [{
            "id": repo_data.get("id"),
            "name": repo_data.get("name"),
            "full_name": repo_data.get("full_name"),
            "description": repo_data.get("description"),
            "stargazers_count": repo_data.get("stargazers_count"),
            "forks_count": repo_data.get("forks_count"),
            "open_issues_count": repo_data.get("open_issues_count"),
            "language": repo_data.get("language"),
            "created_at": repo_data.get("created_at"),
            "updated_at": repo_data.get("updated_at"),
            "html_url": repo_data.get("html_url"),
        }]

    @dlt.resource(name="cfg_issues", write_disposition="replace")
    def get_issues():
        flattened = []
        for issue in issues_data:
            flat_issue = {
                "id": issue.get("id"),
                "number": issue.get("number"),
                "title": issue.get("title"),
                "state": issue.get("state"),
                "created_at": issue.get("created_at"),
                "user_login": issue.get("user", {}).get("login") if issue.get("user") else None,
                "repo_name": repo_name,  # Track which repo
            }
            flattened.append(flat_issue)
        yield flattened

    result = loader.run(get_repo_info(), get_issues())

    return json.dumps({
        "load_result": result,
        "repo_loaded": repo_name,
        "verifications": {
            "repo_info": loader.verify_table("cfg_repo_info"),
            "issues": loader.verify_table("cfg_issues")
        },
        "note": "Configurable source - try with different repos!"
    }, indent=2, default=str)
';
