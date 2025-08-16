# pip install simple-justwatch-python-api httpx

from simplejustwatchapi.justwatch import search, offers_for_countries

TITLE     = "Heat"   # change me
COUNTRY   = "GB"
LANG      = "en"
COUNT     = 5
BEST_ONLY = True

def main():
    results = search(TITLE, COUNTRY, LANG, COUNT, BEST_ONLY)

    if not results:
        print(f"No search hits for '{TITLE}' in {COUNTRY}.")
        return

    print(f"🔎 Search hits for '{TITLE}' ({COUNTRY}/{LANG}): {len(results)}")
    for i, r in enumerate(results, 1):
        # Per docs: MediaEntry has entry_id, object_id, object_type, title, release_year, url, etc.
        print(f"{i}. {r.title} ({r.release_year}) [{r.object_type}]  entry_id={r.entry_id}")

    # take first match
    first = results[0]
    print(f"\nFetching offers for entry_id={first.entry_id} …")

    # Per docs: offers_for_countries expects the GraphQL node id = entry_id
    offers_by_country = offers_for_countries(first.entry_id, {COUNTRY}, LANG, BEST_ONLY)
    offers = offers_by_country.get(COUNTRY, []) or []

    if not offers:
        print("❌ No offers found.")
        return

    print("\n✅ Offers:")
    for o in offers:
        # Offer fields per docs
        prov_name = o.package.name if o.package else "Unknown"
        prov_id   = o.package.package_id if o.package else None
        price_str = o.price_string or ""
        print(
            f"- {o.monetization_type:<9} | {o.presentation_type or '-':<3} | "
            f"{price_str:<10} | {prov_name} (id={prov_id}) | {o.url}"
        )

if __name__ == "__main__":
    main()
