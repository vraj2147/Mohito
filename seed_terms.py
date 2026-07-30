"""Generate and load search terms into the database.

Task (c) needs 1000 distinct terms plus one term repeated 500 times. Rather than
1000 arbitrary strings, terms are built from product/topic seeds crossed with
intent modifiers, and each is tagged `transactional` or `informational`.

That tagging is the point: ad rates are only meaningful if the workload contains
queries that genuinely should NOT carry ads. A list of 1000 shopping queries would
report ~100% ad coverage and tell you nothing.

Shared by every stage: v2 reads search_terms directly via v_pending_work, and v3's
loader builds its RabbitMQ messages from the same view. Run this first, whichever
stage you then use.

Usage:
    python seed_terms.py --distinct 1000 --repeat-term "iPhone 16 Pro" --repeats 500
    python seed_terms.py --dry-run --distinct 20
"""


from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import itertools
import random
import sys

from db.store import Store

# Seeds are grouped by kind, because modifiers are not interchangeable across
# them: "buy mortgage" and "second hand flights to new york" are not queries a
# real user types, and the odd SERPs they return would skew the ad-rate metrics.
GOODS = [
    "iphone 16 pro", "iphone 16", "samsung galaxy s25 ultra", "google pixel 10",
    "macbook air m4", "macbook pro 16", "ipad pro", "dell xps 15", "thinkpad x1",
    "airpods pro", "sony wh-1000xm6", "bose quietcomfort", "kindle paperwhite",
    "nintendo switch 2", "ps5 pro", "xbox series x", "steam deck", "quest 4",
    "dyson v15", "robot vacuum", "air fryer", "espresso machine", "instant pot",
    "nespresso pods", "washing machine", "tumble dryer", "dishwasher",
    "electric toothbrush", "hair dryer", "beard trimmer",
    "running shoes", "hiking boots", "winter coat", "waterproof jacket",
    "office chair", "standing desk", "mattress", "memory foam pillow",
    "smart watch", "fitness tracker", "bluetooth speaker", "soundbar",
    "4k tv", "oled tv", "projector", "gaming monitor", "mechanical keyboard",
    "wireless mouse", "usb c hub", "portable ssd", "microsd card",
    "electric bike", "road bike", "treadmill", "dumbbell set", "yoga mat",
]

SERVICES = [
    "car insurance", "home insurance", "pet insurance", "travel insurance",
    "life insurance", "mortgage", "credit card", "savings account",
    "personal loan", "isa", "pension transfer", "conveyancing",
]

TRAVEL = [
    "flights to new york", "flights to tokyo", "hotels in paris",
    "car hire spain", "cruise deals", "package holidays", "ski chalet",
    "villa in portugal",
]

SOFTWARE = [
    "web hosting", "vpn", "domain name", "cloud storage", "password manager",
    "project management software", "crm software", "accounting software",
    "email marketing tool", "website builder",
]

# Modifier sets per kind. A leading "" yields the bare seed term.
PREFIX_MODIFIERS = {
    "goods": ["", "buy", "best", "cheap", "cheapest", "top", "refurbished",
              "second hand", "discount"],
    "services": ["", "best", "cheap", "cheapest", "compare", "top"],
    "travel": ["", "cheap", "cheapest", "best", "last minute", "all inclusive"],
    "software": ["", "best", "cheap", "top", "free"],
}

SUFFIX_MODIFIERS = {
    "goods": ["", "uk", "deals", "review", "reviews", "price", "sale", "near me",
              "2026", "offers"],
    "services": ["quotes", "uk", "comparison", "rates", "for over 50s", "deals"],
    "travel": ["deals", "uk", "2026", "offers", "last minute"],
    "software": ["uk", "pricing", "review", "reviews", "alternatives", "for small business"],
}

# Informational templates, keyed by the kind of topic they make sense for, so we
# never emit "who invented hurricanes" or "how does volcanoes work".
PROCESS_TOPICS = [
    "photosynthesis", "gravity", "the water cycle", "plate tectonics",
    "the immune system", "evolution", "climate change", "electricity",
    "inflation", "machine learning", "quantum computing", "nuclear fusion",
    "erosion", "osmosis", "natural selection", "the greenhouse effect",
    "compound interest", "blockchain", "encryption", "photosynthesis in plants",
]
PROCESS_TEMPLATES = [
    "what is {}", "how does {} work", "why is {} important", "explain {}",
    "{} definition", "examples of {}", "stages of {}",
]

EVENT_TOPICS = [
    "the roman empire", "the french revolution", "the great depression",
    "the cold war", "the silk road", "the industrial revolution",
    "the renaissance", "world war 1", "the moon landing", "the black death",
    "the ottoman empire", "the ming dynasty", "the berlin wall",
]
EVENT_TEMPLATES = [
    "what was {}", "history of {}", "when did {} start", "causes of {}",
    "why did {} end", "timeline of {}", "who ruled during {}",
]

INVENTION_TOPICS = [
    "penicillin", "the telephone", "the printing press", "the light bulb",
    "the steam engine", "the transistor", "the aeroplane", "the microscope",
    "vaccines", "antibiotics", "the internet", "the telescope",
]
INVENTION_TEMPLATES = [
    "who invented {}", "when was {} invented", "history of {}",
    "how was {} discovered", "why was {} important",
]

THING_TOPICS = [
    "black holes", "dna", "volcanoes", "hurricanes", "the solar system",
    "the periodic table", "coral reefs", "glaciers", "tsunamis", "earthquakes",
    "the amazon rainforest", "antarctica", "mount everest", "the sahara",
]
THING_TEMPLATES = [
    "what are {}", "how do {} form", "types of {}", "facts about {}",
    "where are {} found", "why are {} important",
]


SEEDS_BY_KIND = {
    "goods": GOODS,
    "services": SERVICES,
    "travel": TRAVEL,
    "software": SOFTWARE,
}

TOPIC_SETS = (
    (PROCESS_TOPICS, PROCESS_TEMPLATES, "process"),
    (EVENT_TOPICS, EVENT_TEMPLATES, "event"),
    (INVENTION_TOPICS, INVENTION_TEMPLATES, "invention"),
    (THING_TOPICS, THING_TEMPLATES, "thing"),
)


def generate(n: int, seed: int = 7) -> list[dict]:
    """Build `n` distinct terms, roughly 70% transactional / 30% informational.

    Modifiers are applied only within a compatible kind, so every emitted term is
    one a real user could plausibly type.
    """
    rng = random.Random(seed)

    transactional = []
    for kind, seeds in SEEDS_BY_KIND.items():
        combos = itertools.product(seeds, PREFIX_MODIFIERS[kind], SUFFIX_MODIFIERS[kind])
        for base, prefix, suffix in combos:
            term = " ".join(p for p in (prefix, base, suffix) if p).strip()
            transactional.append(
                {"term": term, "intent": "transactional", "category": kind}
            )

    informational = []
    for topics, templates, cat in TOPIC_SETS:
        for topic, tpl in itertools.product(topics, templates):
            informational.append(
                {"term": tpl.format(topic), "intent": "informational", "category": cat}
            )

    rng.shuffle(transactional)
    rng.shuffle(informational)

    want_info = int(n * 0.3)
    want_trans = n - want_info
    if want_info > len(informational):
        want_info = len(informational)
        want_trans = n - want_info

    picked = transactional[:want_trans] + informational[:want_info]
    # De-duplicate while preserving the mix, then trim to exactly n.
    seen, out = set(), []
    for row in picked:
        if row["term"] not in seen:
            seen.add(row["term"])
            out.append(row)
    rng.shuffle(out)
    return out[:n]


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed the search_terms table.")
    ap.add_argument("--distinct", type=int, default=1000, help="How many distinct terms (default 1000).")
    ap.add_argument("--repeat-term", default="iPhone 16 Pro", help="Term to scrape repeatedly.")
    ap.add_argument("--repeats", type=int, default=500, help="target_repeats for --repeat-term.")
    ap.add_argument("--locale", default="en-GB")
    ap.add_argument("--dry-run", action="store_true", help="Print terms; do not write to the DB.")
    args = ap.parse_args()

    rows = generate(args.distinct)
    if len(rows) < args.distinct:
        print(
            f"WARNING: only {len(rows)} unique terms available from the seed lists "
            f"(asked for {args.distinct}). Add more PRODUCTS/MODIFIERS/TOPICS.",
            file=sys.stderr,
        )

    for r in rows:
        r["locale"] = args.locale
        r["target_repeats"] = 1
        r["priority"] = 0

    # The repeated term is one row carrying its whole workload, and gets priority
    # so it is not starved if the run is cut short.
    repeat_row = {
        "term": args.repeat_term,
        "locale": args.locale,
        "target_repeats": args.repeats,
        "priority": 10,
        "category": "product",
        "intent": "transactional",
    }

    by_intent = {}
    for r in rows:
        by_intent[r["intent"]] = by_intent.get(r["intent"], 0) + 1

    print(f"Distinct terms : {len(rows)}  {by_intent}")
    print(f"Repeated term  : {args.repeat_term!r} x {args.repeats}")
    print(f"Total scrapes  : {len(rows) + args.repeats}")

    if args.dry_run:
        for r in rows[:25]:
            print(f"  [{r['intent'][:5]}] {r['term']}")
        print("  …" if len(rows) > 25 else "")
        return 0

    with Store() as store:
        n = store.upsert_terms(rows + [repeat_row])
        pending = store.pending_work()
        total = sum(int(p["remaining"]) for p in pending)
    print(f"\nUpserted {n} rows. Pending work: {total} scrapes across {len(pending)} terms.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
