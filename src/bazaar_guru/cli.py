"""Small command entrypoints used by scaffold containers."""

import argparse


def main() -> int:
    parser = argparse.ArgumentParser(prog="bazaar-guru")
    parser.add_argument("role", choices=("collector", "worker"))
    args = parser.parse_args()
    message = "pipeline behavior is intentionally pending a later phase"
    print(f"{args.role} scaffold is installed; {message}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
