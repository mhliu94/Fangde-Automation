class FangdeError(RuntimeError):
    """Base error for expected automation failures."""


class ConfigurationError(FangdeError):
    """Configuration is missing or invalid."""


class DeviceError(FangdeError):
    """ADB or emulator validation failed."""


class UiError(FangdeError):
    """The expected application UI could not be reached."""


class SafetyError(FangdeError):
    """A fail-closed safety check prevented an action."""
