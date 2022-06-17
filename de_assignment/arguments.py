import sys

from de_assignment.entities import Arguments


def parse_arguments() -> Arguments:
    if len(sys.argv) != 3:
        sys.exit(1)

    return Arguments(
        input_path=sys.argv[1],
        output_path=sys.argv[2],
    )
