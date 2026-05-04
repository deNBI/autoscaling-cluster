"""
Exception classes for the cluster module.
"""


class ClusterError(Exception):
    """Base exception for cluster-related errors."""

    pass


class APIError(ClusterError):
    """Exception raised for API errors."""

    pass


class AuthError(ClusterError):
    """Exception raised for authentication errors."""

    pass


class ConfigError(ClusterError):
    """Exception raised for configuration errors."""

    pass


class FlavorError(ClusterError):
    """Exception raised for flavor-related errors."""

    pass


class ScaleError(ClusterError):
    """Exception raised for scaling operation errors."""

    pass


class VersionMismatchError(ClusterError):
    """Exception raised when client and server versions don't match."""

    def __init__(self, client_version: str, server_version: str):
        super().__init__(f"Version mismatch: client {client_version}, server {server_version}")
        self.client_version = client_version
        self.server_version = server_version
