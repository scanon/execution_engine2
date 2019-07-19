from typing import Dict, List
from abc import ABC


class Scheduler(ABC):

    def create_submit(self, params: Dict[str, str]) -> str: ...

    def validate_submit_file(self, submit_file_path) -> bool: ...

    def cleanup_submit_file(self, submit_file_path) -> bool: ...

    def run_submit(self, submit_file_path) -> str: ...

    def get_job_info(self, job_id: str, cluster_id: str = None) -> Dict[str, str]: ...

    def get_user_info(self, user_id: str, projection: List[str] = None) -> Dict[str, str]: ...

    def cancel_job(self, job_id: str) -> bool: ...

    def run_job(self, params: Dict[str, str], submit_file: Dict[str, str] = None) -> str: ...