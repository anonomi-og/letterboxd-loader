# pip install simple-justwatch-python-api httpx
import argparse
from simplejustwatchapi.justwatch import search, offers_for_countries

def main():
    ap = argparse.ArgumentParser(description="Manual JustWatch (unofficial) tester")
    ap.add_argument("title", help="Title to search")
    ap.add_argument("--year", type=int, help="Prefer a match with this release year")
    ap.add_argument("--country", default="GB", help="Country code (default GB)")
    ap.add_argument("--lang", default="en", help="Language code (default en)")
    ap.add_argument("--count", type=int, default=10, help="Max search results")
    ap.add_argument("--best-only", action="store_true", help="Only best quality per provider/type")
    ap.add_argument("--type", choices=["FLATRATE","RENT","BUY","FREE","ADS","CINEMA"], help="Filter by monetization type")
    ap.add_argument("--provider-id", type=int, action="append", help="Only show these provider IDs (can repeat)")
    args = ap.parse_args()

    results = search(args.title, args.country, args.lang, args.count, args.best_only)

    if not results:
        print("No results.")
        return

    # Prefer exact year match if provided, else first
    match = None
    if args.year:
        for r in results:
            if getattr(r, "release_year", None) == args.year:
                match = r; break
    match = match or results[0]

    print(f"🎬 Using: {match.title} ({getattr(match,'release_year',None)}) [{match.object_type}] entry_id={match.entry_id}")

    offers_by_country = offers_for_countries(match.entry_id, {args.country}, args.lang, args.best_only)
    offers = (offers_by_country or {}).get(args.country, []) or []

    if args.type:
        offers = [o for o in offers if o.monetization_type == args.type]
    if args.provider_id:
        offers = [o for o in offers if (o.package and o.package.package_id in set(args.provider_id))]

    if not offers:
        print("No offers (after filters).")
        return

    print("\nOffers:")
    for o in offers:
        prov_name = o.package.name if o.package else "Unknown"
        prov_id   = o.package.package_id if o.package else None
        price_str = o.price_string or ""
        pres      = o.presentation_type or "-"
        print(f"- {o.monetization_type:<9} | {pres:<3} | {price_str:<10} | {prov_name} (id={prov_id}) | {o.url}")

if __name__ == "__main__":
    main()
