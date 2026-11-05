"""Confirmed catalog-write outcomes and dry-run operation plans."""


def perform(stats, dry_run, operation, *args):
    name = operation.__name__
    target = str(args[0]) if args else "unknown"
    if dry_run:
        stats["skipped"] += 1
        stats["planned"].append(
            {"operation": name, "target": target, "arguments": args[1:]}
        )
        return False
    stats["attempted"] += 1
    try:
        changed = operation(*args)
    except Exception as error:
        stats["failed"] += 1
        stats["errors"].append(
            {
                "operation": name,
                "target": target,
                "code": type(error).__name__,
                "message": "Catalog write failed",
            }
        )
        return False
    if changed is False:
        stats["skipped"] += 1
        return False
    stats["succeeded"] += 1
    return True
