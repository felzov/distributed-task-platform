import task_platform


def test_package_is_importable() -> None:
    # Fails if the src layout or the hatchling package path is misconfigured.
    assert task_platform.__name__ == "task_platform"
