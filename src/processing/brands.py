"""
brands.py
─────────
Curated brand lookup for Kenyan FMCG products.

Used by cleaning.py to extract reliable brand names from product name strings
instead of relying on the first-word heuristic.

HOW IT WORKS
────────────
KNOWN_BRANDS: maps each category to a list of (alias_pattern, canonical_name) tuples.
  - alias_pattern : regex that matches any common way the brand appears in a name
  - canonical_name: the single normalised form written to the 'brand' column

COMMODITY_CATEGORIES: categories where brand is intentionally excluded from the SKU.
  For these, cross-store price comparison is at the category+size level (e.g.
  "1kg sugar") rather than the brand level, which is appropriate for a basket
  price index.

MAINTENANCE
───────────
Add new brands by appending to the relevant list.
Patterns are matched case-insensitively against the full lowercased product name.
"""

# ─────────────────────────────────────────────────────────────────────────────
# For these categories the SKU will be: category + base_size (brand excluded).
# Rationale: stores stock different sugar/flour/rice brands, and you want
# "cheapest 1 kg sugar" across stores, not brand-to-brand matching which
# would exclude most rows from the pivot.
# ─────────────────────────────────────────────────────────────────────────────
COMMODITY_CATEGORIES = {
    'Sugar',
    'Rice',
    'Wheat Flour',
    'Maize Meal',
    'Cooking Oil',
    'Bread',
}

# ─────────────────────────────────────────────────────────────────────────────
# For branded categories, we extract the brand via this lookup.
# Each entry: (regex_pattern, canonical_brand_name)
# Patterns are tried in order — put more specific patterns first.
# ─────────────────────────────────────────────────────────────────────────────
KNOWN_BRANDS: dict[str, list[tuple[str, str]]] = {

    'Milk': [
        # ── existing brands ──────────────────────────────────────────────────
        (r'brookside',          'Brookside'),
        (r'daima',              'Daima'),
        (r'fresha',             'Fresha'),
        (r'tuzo',               'Tuzo'),
        (r'\bkcc\b|k\.c\.c',   'KCC'),
        (r'molo',               'Molo'),
        (r'buzeki',             'Buzeki'),
        (r'highland',           'Highland'),
        (r'\bbio\b',            'BioFood'),   # "bio fresh milk", "bio uht" etc.
        (r'eltee',              'Eltee'),
        # ── added from price_history.csv ────────────────────────────────────
        (r'\b4us\b',            '4US'),
        (r'aptamil',            'Aptamil'),
        (r'\bilara\b',          'Ilara'),
        (r'\bkara\b',           'Kara'),
        (r'\blato\b',           'Lato'),
        (r'\bmiksi\b',          'Miksi'),
        (r'mt\.?\s*kenya',      'Mt. Kenya'),
        (r'naivas',             'Naivas'),
        (r"nestle'?",           'Nestle'),
        (r'nu\s*ziwa',          'Nu Ziwa'),
        (r'quick\s*choice',     'Quick Choice'),
        (r'first\s*choice',     'First Choice'),
        (r'\broyal\b',          'Royal'),
        (r'\bvito\b',           'Vito'),
    ],

    'Bread': [
        (r'supa\s*loaf|supa\b', 'Supa Loaf'),   # "supa loaf", "supa brown"
        (r'broadway',           'Broadway'),     # covers both "broadway" & "broadways"
        (r'\bfestive\b',        'Festive'),
        (r'\bbudget\b',         'Budget'),
        (r'\bfm\b',             'FM'),
        (r'fresh\s+wholemeal',  'Fresh'),
    ],

    'Cooking Oil': [
        (r'captain\s*cook',     'Captain Cook'),
        (r'fresh\s*fri',        'Fresh Fri'),
        (r'golden\s*(?:fry|drop)', 'Golden Fry'),
        (r'\bavena\b',          'Avena'),
        (r'\bbahari\b',         'Bahari'),
        (r'\bdola\b',           'Dola'),
        (r'\belianto\b',        'Elianto'),
        (r'\bflora\b',          'Flora'),
        (r'\bkentaste\b',       'Kentaste'),
        (r'\bkimbo\b',          'Kimbo'),
        (r'\bmasterchef\b',     'Masterchef'),
        (r'\bparachute\b',      'Parachute'),
        (r'\bpika\b',           'Pika'),
        (r'\bpradip\b',         'Pradip'),
        (r'\brina\b',           'Rina'),
        (r'\bsalit\b',          'Salit'),
        (r'\bufuta\b',          'Ufuta'),
    ],

    'Maize Meal': [
        (r'natures?\s+equatorial',  'Natures Equatorial'),
        (r"winnie'?s",              "Winnie's Pure Health"),
        (r'\b210\b',                '210'),
        (r'\bajab\b',               'Ajab'),
        (r'\bamaize\b',             'Amaize'),
        (r'\bdola\b',               'Dola'),
        (r'\bhostess\b',            'Hostess'),
        (r'\bjogoo\b',              'Jogoo'),
        (r'\blea\b',                'Lea'),
        (r'\bmama\b',               'Mama'),
        (r'\bmasterchef\b',         'Masterchef'),
        (r'\bndovu\b',              'Ndovu'),
        (r'\bnice\b',               'Nice'),
        (r'\bpembe\b',              'Pembe'),
        (r'\braha\b',               'Raha'),
        (r'\bsoko\b',               'Soko'),
        (r'\bspenza\b',             'Spenza'),
        (r'\btupike\b',             'Tupike'),
    ],

    'Rice': [
        (r'royal\s*umbrella',       'Royal Umbrella'),
        (r'morning\s*harvest',      'Morning Harvest'),
        (r"winnie'?s",              "Winnie's Pure Health"),
        (r'\bs\s*&\s*s\b',          'S&S'),
        (r'\b224\b',                '224'),
        (r'\bcil\b',                'CIL'),
        (r'\bdaawat\b',             'Daawat'),
        (r'\bfalcon\b',             'Falcon'),
        (r'\bfarmnaivas\b',         'Farmnaivas'),
        (r'\bjamii\b',              'Jamii'),
        (r'\bkcl\b',                'KCL'),
        (r'\bkings\b',              'Kings'),
        (r'\bkpl\b',                'KPL'),
        (r'\bnaivas\b',             'Naivas'),
        (r'\bnice\b',               'Nice'),
        (r'\bnutrameal\b',          'Nutrameal'),
        (r'\bpearl\b',              'Pearl'),
        (r'\bpriceless\b',          'Priceless'),
        (r'\branee\b',              'Ranee'),
        (r'\bsunrice\b',            'Sunrice'),
        (r'\btango\b',              'Tango'),
    ],

    'Sugar': [
        (r'\bclovers\b',            'Clovers'),
        (r'\beconomy\b',            'Economy'),
        (r'\bjomu\b',               'Jomu'),
        (r'\bkabras\b',             'Kabras'),
        (r'\bmumias\b',             'Mumias'),
        (r'\bnaivas\b',             'Naivas'),
        (r'\bnutrameal\b',          'Nutrameal'),
        (r'\bzesta\b',              'Zesta'),
    ],

    'Wheat Flour': [
        (r'ready\s*bake',           'Ready Bake'),
        (r"winnie'?s",              "Winnie's Pure Health"),
        (r'\b210\b',                '210'),
        (r'\bajab\b',               'Ajab'),
        (r'\bbutterfly\b',          'Butterfly'),
        (r'\bdola\b',               'Dola'),
        (r'\belliots?\b',           'Elliots'),
        (r'\bexe\b',                'Exe'),
        (r'\bgrainmill\b',          'Grainmill'),
        (r'\bjogoo\b',              'Jogoo'),
        (r'\bkamal\b',              'Kamal'),
        (r'\bkamili\b',             'Kamili'),
        (r'\bkentaste\b',           'Kentaste'),
        (r'\blea\b',                'Lea'),
        (r'\blotus\b',              'Lotus'),
        (r'\bmasterchef\b',         'Masterchef'),
        (r'\bndovu\b',              'Ndovu'),
        (r'\boboma\b',              'Oboma'),
        (r'\bpembe\b',              'Pembe'),
        (r'\bpendo\b',              'Pendo'),
        (r'\bphulka\b',             'Phulka'),
        (r'\braha\b',               'Raha'),
        (r'\brimwabi\b',            'Rimwabi'),
        (r'\bsoko\b',               'Soko'),
        (r'\bumix\b',               'Umix'),
        (r'\bumoja\b',              'Umoja'),
        (r'\bunga\b',               'Unga'),
        (r'\bvitafla\b',            'Vitafla'),
        (r'\bzesta\b',              'Zesta'),
    ],
}