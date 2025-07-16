import os
import requests
from google.cloud import secretmanager


class SecretManagerClient:
    def __init__(self, project_id=None):
        self.project_id = project_id or os.getenv("GCP_TARGET_PROJECT")
        if not self.project_id:
            raise ValueError("GCP_TARGET_PROJECT environment variable must be set")

        self.client = secretmanager.SecretManagerServiceClient()

    def get_secret(self, secret_name):
        """
        Retrieve the latest version of a secret from Google Secret Manager.
        Args:
            secret_name (str): Name of the secret.

        Returns:
            str: The secret value.
        """
        secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = self.client.access_secret_version(request={"name": secret_path})
        return response.payload.data.decode("UTF-8")


class DLCTaxonomyClient:
    BASE_URL = os.getenv("DLC_BASE_URI")

    def __init__(self, api_key=None, user_agent='Foundations DBT Runner'):
        self.api_key = api_key or SecretManagerClient().get_secret('DLC_API_KEY')
        self.user_agent = user_agent

        if not self.api_key:
            raise ValueError("DLC_API_KEY secret not found in Secret Manager")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": self.user_agent,
            "Content-Type": "application/json"
        }

        self._taxonomy_cache = None

    def fetch_taxonomy_tags(self):
        """
        Fetch all taxonomy tags from DLC.

        Returns:
            dict: Mapping of platformTag -> taxonomyTagId.
        """
        response = requests.get(f"{self.BASE_URL}/taxonomy", headers=self.headers)
        response.raise_for_status()
        taxonomy = response.json()

        # Extract only PIPELINE_PLATFORM tags
        return {
            tag['name']: tag['id']
            for tag in taxonomy
            if tag.get('category') == "PIPELINE_PLATFORM"
        }

    def get_taxonomy_tag_id(self, platform_tag):
        """
        Get taxonomy tag id for a platform tag.

        Returns:
            int: taxonomyTagId
        """
        if self._taxonomy_cache is None:
            self._taxonomy_cache = self.fetch_taxonomy_tags()

        taxonomy_tag_id = self._taxonomy_cache.get(platform_tag)

        if taxonomy_tag_id is None:
            raise ValueError(f"No taxonomyTagId found for platformTag '{platform_tag}'")

        return taxonomy_tag_id


class DMSClient:
    BASE_URL = f"{os.getenv('DMS_BASE_URI')}/documents/get?keyed=true&json=true"

    def __init__(self, api_key=None):
        self.is_prod = os.getenv("GCP_TARGET_PROJECT") == "foundations-prd-1sm9"
        self.api_key = api_key or SecretManagerClient().get_secret('DMS_API_KEY')
        if not self.api_key:
            raise ValueError(f"DMS_API_KEY secret not found in Secret Manager")

        self.headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json"
        }

    def fetch_model_and_pool_name(self, job_id):
        """
        Fetch model name using fetch_model_by_job_id() and generate the string:
        +{model_name} --vars '{"pool_name": "{model_name_with_underscores}", "scala_job_id": "{job_id}"}'
        """
        model_name = self._fetch_model_by_job_id(job_id, self.is_prod)
        pool_name = job_id.replace("/", "_")

        return f"+{model_name} --vars '{{\"pool_name\": \"{pool_name}\", \"scala_job_id\": \"{job_id}\"}}'"

    def _fetch_model_by_job_id(self, job_id, is_prod):
        """
        Process:
        1. Fetch job config from DMS.
        2. If platformTag exists, fetch shared platform config via taxonomy ID.
        3. Merge configs, giving priority to job config.
        4. Extract 'foundations-table-name'.

        If platformTag is missing, no shared config is fetched — only job config is used.
        """
        job_config = self._fetch_job_config(job_id, is_prod)
        platform_tag = job_config.get('platformTag')

        if not platform_tag:
            print(f"Warning: No platformTag found in job config for jobId {job_id}. Using job config only.")
            merged_config = job_config
        else:
            dlc_client = DLCTaxonomyClient()
            taxonomy_tag_id = dlc_client.get_taxonomy_tag_id(platform_tag)
            shared_config = self._fetch_shared_platform_config(taxonomy_tag_id, is_prod)

            # Deep merge configs, giving priority to job_config over shared_config
            merged_config = self._deep_merge_with_priority(shared_config, job_config)

        model = merged_config.get('foundations-table-name')
        if not model:
            raise ValueError(f"No 'foundations-table-name' found in config for jobId {job_id}")

        return model

    def _fetch_job_config(self, job_id, is_prod):
        search_request = {
            "externalIds": [{"externalIdType": "JobId", "value": job_id}],
            "documentType": "PipelineJob",
            "includeUnreleased": not is_prod,
            "latestOnly": True
        }

        response = requests.post(self.BASE_URL, json=search_request, headers=self.headers)
        response.raise_for_status()

        records = response.json()
        if not records:
            raise ValueError(f"No job config found for jobId {job_id}")

        return records[0].get('json', {})

    def _deep_merge_with_priority(self, base, override):
        merged = base.copy()

        for key, override_value in override.items():
            if (
                    key in merged
                    and isinstance(merged[key], dict)
                    and isinstance(override_value, dict)
            ):
                merged[key] = self._deep_merge_with_priority(merged[key], override_value)
            else:
                merged[key] = override_value

        return merged

    def _fetch_shared_platform_config(self, taxonomy_tag_id, is_prod):
        search_request = {
            "externalIds": [{"externalIdType": "PipelinePlatformId", "value": taxonomy_tag_id}],
            "documentType": "SharedPipelineJob",
            "includeUnreleased": not is_prod,
            "latestOnly": True
        }

        response = requests.post(self.BASE_URL, json=search_request, headers=self.headers)
        response.raise_for_status()

        records = response.json()
        if not records:
            raise ValueError(f"No shared config found for taxonomyTagId {taxonomy_tag_id}")

        return records[0].get('json', {})


