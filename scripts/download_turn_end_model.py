"""Download only pinned multilingual inference assets; never download at gateway startup."""

import argparse
import json
import sys
from pathlib import Path
from urllib.request import urlopen

from realtime_voice.turn_end.model import (
    MODEL_ASSET_SHA256,
    MODEL_REPOSITORY,
    MODEL_REVISION,
    asset_sha256,
    validate_model_assets,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("models/turn-end-livekit"))
    parser.add_argument(
        "--check", action="store_true", help="validate local assets without downloading"
    )
    args = parser.parse_args()
    try:
        validate_model_assets(args.output)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        if args.check:
            print(f"Model check failed: {error}", file=sys.stderr)
            raise SystemExit(1) from None
    else:
        print(f"Model already ready: {args.output.resolve()}")
        return

    for name, expected in MODEL_ASSET_SHA256.items():
        path = args.output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_file() and asset_sha256(path) == expected:
            print(f"Reusing {name}: {path.stat().st_size} bytes", flush=True)
            continue
        temporary = path.with_suffix(path.suffix + ".download")
        try:
            with (
                urlopen(
                    f"https://huggingface.co/{MODEL_REPOSITORY}/resolve/{MODEL_REVISION}/{name}",
                    timeout=60,
                ) as response,
                temporary.open("wb") as target,
            ):
                while chunk := response.read(1024 * 1024):
                    target.write(chunk)
            if asset_sha256(temporary) != expected:
                raise ValueError(f"downloaded model asset hash mismatch: {name}")
            temporary.replace(path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        print(f"Downloaded {name}: {path.stat().st_size} bytes", flush=True)

    (args.output / "manifest.json").write_text(
        json.dumps(
            {
                "repository": MODEL_REPOSITORY,
                "revision": MODEL_REVISION,
                "sha256": MODEL_ASSET_SHA256,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    validate_model_assets(args.output)
    print(f"Model ready: {args.output.resolve()}")


if __name__ == "__main__":
    main()
