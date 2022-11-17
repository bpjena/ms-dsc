from pathlib import Path
from typing import Union


def get_relative_file_path(
    file_path: Union[str, Path], caller_file_path: Union[str, Path] = None
) -> Path:
    """
    Instead of using cd into each subdirectory under 'python/ to run a file'
    this is a bridging function to get the file path and remain backwards compatible
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)
    if caller_file_path is None:
        caller_file_path = __file__
    if isinstance(caller_file_path, str):
        caller_file_path = Path(caller_file_path)

    if file_path.exists():
        confirmed_file_path = file_path
    else:
        file_path = caller_file_path.parent.joinpath(file_path)
        if not file_path.exists():
            raise ValueError(f"{file_path} does not exist")
        else:
            confirmed_file_path = file_path

    return confirmed_file_path
