"""
Exception classes for the cloud module.
"""


class CloudError(Exception):
    """Base exception for cloud-related errors."""

    pass


class APIError(CloudError):
    """Exception raised for API errors."""

    pass


class AuthError(CloudError):
    """Exception raised for authentication errors."""

    pass


class PlaybookError(CloudError):
    """Exception raised for Ansible playbook errors."""

    pass
