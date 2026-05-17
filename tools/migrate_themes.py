"""Migrate ticker_themes.json from flat strings to hierarchical paths.

Applies:
  1. OLD_TO_NEW — mapping table for each of the ~324 legacy theme strings
  2. TICKER_OVERRIDES — explicit assignments for Singletons / Uncategorized / Memes
  3. MULTI_THEME_ADDITIONS — secondary L1 paths for diversified tickers

Reads data/ticker_themes.pre_migration.json and writes data/ticker_themes.json.
Validates every output path against config/theme_taxonomy.yaml.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Mapping: legacy theme string → new hierarchical path (slash-delimited)
# Empty string = drop this theme; ticker may end up with no themes (then we
# fall back to Singleton).
# ─────────────────────────────────────────────────────────────────────────────
OLD_TO_NEW: dict[str, str] = {
    # ── AI cluster ────────────────────────────────────────────────────────
    "AI - Software & Analytics":          "AI / Software & Analytics",
    "AI - Software Services":             "AI / Software & Analytics",
    "AI - Data Center & Cloud":           "AI / Data Center / Cloud & Hyperscalers",
    "AI - Data Center & Cloud Services":  "AI / Data Center / Cloud & Hyperscalers",
    "AI - Data Center Components":        "AI / Data Center / Components",
    "AI - Data Center Power":             "AI / Data Center / Power & Cooling",
    "AI - Infra / Connectivity":          "AI / Data Center / Networking & Connectivity",
    "AI - Infra / Core Chips":            "AI / Data Center / Chips & Processors",
    "AI - Infra / Optics":                "AI / Data Center / Optics",
    "AI - Infra / Power/Cooling":         "AI / Data Center / Power & Cooling",
    "AI - Memory & Storage":              "AI / Data Center / Memory",
    "AI - Optics":                        "AI / Data Center / Optics",
    "AI - Optoelectronics":               "AI / Data Center / Optics",
    "AI - Processors":                    "AI / Data Center / Chips & Processors",
    "AI - Robotics":                      "Robotics / Industrial Automation",
    "AI - Robotics & Automation":         "Robotics / Industrial Automation",
    "AI - Security & Surveillance":       "Cybersecurity / Surveillance & IoT",
    "AI - Semiconductor Processing":      "AI / Semiconductor Equipment",
    "AI - Semiconductor Testing":         "AI / Semiconductor Equipment",
    "AI - Biotech & Drug Discovery":      "AI / Drug Discovery",
    "AI - Drug Discovery":                "AI / Drug Discovery",
    "AI - Conversational Avatars":        "AI / Hardware / Avatars",

    # ── Hardware (AR/VR) ──────────────────────────────────────────────────
    "Hardware / AR & Smart Glasses":      "AI / Hardware / AR Glasses",
    "AR/VR / Software":                   "AI / Hardware / AR Glasses",

    # ── Semiconductors (non-AI) ───────────────────────────────────────────
    "Semiconductor / IP Licensing":       "Semiconductors / IP Licensing",
    "Semiconductor / Materials":          "Semiconductors / Materials",
    "Semiconductor Processing":           "Semiconductors / Manufacturing & Equipment",
    "Semiconductors / Display Drivers":   "Semiconductors / Display Drivers",
    "Semiconductors / Specialty Foundry": "Semiconductors / Specialty Foundry",
    "Connectivity / High-Speed Video & Data": "Telecom / High-Speed Video & Data",
    "Connectivity / RF Filters":          "Telecom / RF Filters",

    # ── Software & Internet ───────────────────────────────────────────────
    "Software / Data & Analytics":        "Software & Internet / Data & Analytics",
    "Software / Software Services":       "Software & Internet / SaaS & Vertical",
    "Software Services":                  "Software & Internet / SaaS & Vertical",
    "Software / DevOps":                  "Software & Internet / DevOps & Data",
    "Software / PropTech":                "Software & Internet / PropTech",
    "Software / Social Media Management": "Software & Internet / Social & Communication",
    "Software / RT3D & Gaming Engines":   "Software & Internet / Gaming",
    "IT Services & Consulting":           "Software & Internet / BPO & IT Services",
    "Business Process Outsourcing (BPO)": "Software & Internet / BPO & IT Services",
    "Business Services / Uniform Rental & Workplace Supplies": "Industrials / Workplace Services & Uniforms",
    "Human Capital Management / Benefits Tech": "Software & Internet / SaaS & Vertical",
    "EdTech / Online Learning":           "Education / EdTech & Online Learning",
    "Education / Higher Education Services": "Education / Higher Education",
    "Ads Tech & Marketing":               "Software & Internet / Ads & Marketing Tech",
    "Digital Entertainment / Streaming":  "Software & Internet / Streaming",
    "Digital Entertainment / iGaming":    "Software & Internet / iGaming",
    "Digital Entertainment / Dating Services": "Software & Internet / Dating",
    "Gaming":                             "Software & Internet / Gaming",
    "Social Media / Political Sentiment": "Software & Internet / Social & Communication",
    "E-commerce and Digital Retail":      "Software & Internet / E-commerce",
    "E-commerce / Automotive Marketplace": "Retail (Multi-Category) / Auto Marketplace",
    "E-commerce / Event Ticketing":       "Retail (Multi-Category) / Event Ticketing",
    "E-commerce / Luxury Resale":         "Retail (Multi-Category) / Luxury Resale",
    "E-commerce / Personal Styling":      "Retail (Multi-Category) / Personal Styling",
    "E-commerce / Southeast Asia":        "Retail (Multi-Category) / Southeast Asia",
    "Luxury Retail / Home Furnishings":   "Consumer / Home Goods & Furnishings",
    "Home Furnishings":                   "Consumer / Home Goods & Furnishings",
    "Home Services Marketplace":          "Real Estate / Home Services Marketplace",
    "Supply Chain / Traceability":        "Logistics / Supply Chain Tech",

    # ── Cybersecurity ─────────────────────────────────────────────────────
    "Cybersecurity":                      "Cybersecurity / Generalist",
    "Smart Home / IoT Security":          "Cybersecurity / Surveillance & IoT",
    "Security / Surveillance Technology": "Cybersecurity / Surveillance & IoT",
    "Fintech / Payment Security":         "Cybersecurity / Data Security",

    # ── Fintech & Crypto ──────────────────────────────────────────────────
    "Fintech & Digital Payments":         "Fintech & Crypto / Payments",
    "Fintech / Digital Payments":         "Fintech & Crypto / Payments",
    "Fintech / Cross-border Payments":    "Fintech & Crypto / Payments",
    "Fintech / Infrastructure":           "Fintech & Crypto / Payments",
    "Fintech / Buy Now Pay Later":        "Fintech & Crypto / BNPL",
    "Fintech / Mortgage Lending":         "Fintech & Crypto / Mortgage Lending",
    "Fintech / Wealth Management":        "Fintech & Crypto / Wealth & Asset Management",
    "Financial Services / Consumer Finance":   "Fintech & Crypto / Consumer Finance",
    "Financial Services / Wealth Management":  "Fintech & Crypto / Wealth & Asset Management",
    "Financial Services / Mortgage Lending":   "Fintech & Crypto / Mortgage Lending",
    "Financial Services / Investment Banking": "Fintech & Crypto / Investment Banking",
    "Financial Services / Specialty Lending":  "Fintech & Crypto / Specialty Lending",
    "Financial Services / Private Equity Proxy": "Fintech & Crypto / Private Equity Proxy",
    "Financial Services / Digital Assets":     "Fintech & Crypto / Crypto / Infrastructure",
    "Financials / Insurance Brokerage":   "Fintech & Crypto / Insurance & Insurtech",
    "Financials - Insurance Brokerage":   "Fintech & Crypto / Insurance & Insurtech",
    "Financials / Insurtech":             "Fintech & Crypto / Insurance & Insurtech",
    "Financials / SPAC":                  "Fintech & Crypto / SPAC",
    "Financials / Specialty Lending":     "Fintech & Crypto / Specialty Lending",
    "Financials / Argentina":             "Geographic / Argentina",
    "Private Credit":                     "Fintech & Crypto / Private Credit",
    "Cryptocurrency":                     "Fintech & Crypto / Crypto / Generalist",
    "Cryptocurrency / Bitcoin Proxy":     "Fintech & Crypto / Crypto / Bitcoin Proxy",
    "Cryptocurrency / Mining":            "Fintech & Crypto / Crypto / Mining",
    "Cryptocurrency / Infrastructure":    "Fintech & Crypto / Crypto / Infrastructure",

    # ── Biotech ───────────────────────────────────────────────────────────
    "Biotech / Oncology":                 "Biotech / Oncology / General",
    "Biotechnology / Oncology":           "Biotech / Oncology / General",
    "Biotech / Oncology Antibodies":      "Biotech / Oncology / Antibodies",
    "Biotech / Oncology Immunotherapy":   "Biotech / Oncology / Immuno-Oncology",
    "Biotech / Immuno-Oncology":          "Biotech / Oncology / Immuno-Oncology",
    "Biotech / Precision Oncology":       "Biotech / Oncology / Precision",
    "Biotech / Oncology - RAS Inhibitors":"Biotech / Oncology / RAS Inhibitors",
    "Biotech / Oncology - Oncolytic Viruses": "Biotech / Oncology / Oncolytic Viruses",
    "Biotech / Rare Disease":             "Biotech / Rare Disease",
    "Biotech / Rare Diseases":            "Biotech / Rare Disease",
    "Biotechnology / Rare Diseases":      "Biotech / Rare Disease",
    "Biotech / Rare Endocrine Diseases":  "Biotech / Rare Disease",
    "Biotech / Immunology":               "Biotech / Immunology",
    "Biotechnology / Immunology":         "Biotech / Immunology",
    "Biotech / Autoimmune":               "Biotech / Immunology",
    "Biotech / Neuroscience":             "Biotech / Neuroscience",
    "Biotech - Neuroscience":             "Biotech / Neuroscience",
    "Biotech / Neurodegenerative":        "Biotech / Neuroscience",
    "Biotechnology / CNS":                "Biotech / CNS",
    "Biotech / Metabolic & Obesity":      "Biotech / Metabolic & Obesity",
    "Biotech / Cell Therapy":             "Biotech / Cell & Gene Therapy",
    "Biotech / Cell & Gene Therapy":      "Biotech / Cell & Gene Therapy",
    "Biotech / Gene Therapy":             "Biotech / Cell & Gene Therapy",
    "Biotechnology / Gene Therapy":       "Biotech / Cell & Gene Therapy",
    "Biotech / Gene Editing":             "Biotech / Gene Editing",
    "Biotechnology / Gene Editing":       "Biotech / Gene Editing",
    "Biotech / RNAi":                     "Biotech / RNAi & Antisense",
    "Biotechnology / Vaccines":           "Biotech / Infectious Disease & Vaccines",
    "Biotechnology / Infectious Disease": "Biotech / Infectious Disease & Vaccines",
    "Biotechnology / Anti-Infectives":    "Biotech / Anti-Infectives",
    "Biotechnology / mRNA Technology":    "Biotech / Infectious Disease & Vaccines",
    "Biotechnology / Nephrology":         "Biotech / Nephrology",
    "Biotech / Cardiovascular":           "Biotech / Cardiovascular",
    "Biotech / Respiratory":              "Biotech / Respiratory",
    "Biotech / Ophthalmology":            "Biotech / Ophthalmology",
    "Biotech / Dermatology":              "Biotech / Dermatology",
    "Biotech / GI Disorders":             "Biotech / GI Disorders",
    "Biotech / Gastrointestinal":         "Biotech / GI Disorders",
    "Biotech / Specialty Pharma":         "Biotech / Specialty Pharma",
    "Biotech / Psychedelic Medicine":     "Biotech / Psychedelic Medicine",
    "Biotech / Drug Delivery":            "Biotech / Drug Delivery",
    "Biotech / Drug Discovery":           "Biotech / Drug Discovery Platform",
    "Biotech / Next-Gen Biologics":       "Biotech / Drug Discovery Platform",
    "Biotech / Targeted Protein Degradation": "Biotech / Targeted Protein Degradation",
    "Biotechnology / Protein Degradation":"Biotech / Targeted Protein Degradation",
    "Biotech / Precision Medicine":       "Biotech / Drug Discovery Platform",
    "Biotech / Complement System":        "Biotech / Complement System",
    "Biotech / Complement-mediated Diseases": "Biotech / Complement System",
    "Biotech / Medical Countermeasures":  "Biotech / Infectious Disease & Vaccines",
    "Biopharma / Uro-Oncology":           "Biotech / Oncology / General",
    "Life Sciences / Genomics":           "Biotech / Genomics & Diagnostics Platforms",
    "Life Sciences / Spatial Biology":    "Biotech / Genomics & Diagnostics Platforms",
    "Genomics / Long-Read Sequencing":    "Biotech / Genomics & Diagnostics Platforms",
    "Food & Animal Safety / Genomics":    "Biotech / Genomics & Diagnostics Platforms",

    # ── Healthcare (services / payers / providers) ───────────────────────
    "Healthcare / Diagnostics":           "Healthcare / Diagnostics & Labs",
    "Healthcare / Data & Imaging":        "Healthcare / Imaging",
    "Healthcare / Data & Analytics":      "Healthcare / Data & Analytics",
    "Healthcare / AI-Driven Managed Care":"Healthcare / Managed Care",
    "Healthcare / Skilled Nursing Facilities": "Healthcare / Skilled Nursing",
    "Healthcare / Home Health Services":  "Healthcare / Home & Behavioral Health",
    "Healthcare / Behavioral Health Services": "Healthcare / Home & Behavioral Health",
    "Healthcare / Staffing & Workforce Solutions": "Healthcare / Staffing",
    "Healthcare / Clinical Research Organizations (CRO)": "Healthcare / CRO & Clinical Services",
    "Healthcare / E-commerce & Pharmacy": "Healthcare / Pharmacy & E-commerce",
    "Healthcare / Women's Health":        "Healthcare / Women's Health",
    "Healthcare / Animal Health":         "Healthcare / Animal Health",
    "Healthcare / Hospitals":             "Healthcare / Hospitals & Facilities",
    "Healthcare / Cardiovascular":        "Healthcare / Specialty Pharmaceuticals",
    "Healthcare / Ophthalmic Pharmaceuticals": "Healthcare / Specialty Pharmaceuticals",
    "Healthcare / China":                 "Geographic / China",
    "Healthcare / Latin America":         "Geographic / Latin America",
    "Health":                             "Healthcare / Managed Care",

    # ── MedTech ───────────────────────────────────────────────────────────
    "Medical Devices / Diagnostics":      "MedTech / Diagnostics",
    "MedTech / Diagnostics":              "MedTech / Diagnostics",
    "MedTech / Diabetes Management":      "MedTech / Diabetes",
    "MedTech / Wearables":                "MedTech / Wearables",
    "Medical Devices / Cardiovascular":   "MedTech / Cardiovascular",
    "Medical Devices / Oncology":         "MedTech / Oncology Devices",
    "Medical Devices / Aesthetics":       "MedTech / Aesthetics",
    "Medical Devices / Respiratory":      "MedTech / Respiratory",
    "Medical Devices / Spinal Surgery":   "MedTech / Spinal & Orthopedic",
    "Medical Devices / Surgical Robotics":"MedTech / Surgical Robotics",
    "Medical Devices / Dermatology":      "MedTech / Dermatology Devices",
    "Medical Isotopes":                   "MedTech / Diagnostics",

    # ── Clean Energy ──────────────────────────────────────────────────────
    "Solar":                              "Clean Energy / Solar",
    "Solar / Commercial & Industrial":    "Clean Energy / Solar / C&I Install",
    "Solar / Inverters":                  "Clean Energy / Solar / Inverters",
    "Solar / Thin-Film":                  "Clean Energy / Solar / Thin-Film",
    "Fuel Cell":                          "Clean Energy / Fuel Cell & Hydrogen",
    "Batteries":                          "Clean Energy / Batteries & Storage",
    "Consumer Goods / Batteries":         "Clean Energy / Batteries & Storage",
    "Energy Storage / Grid Infrastructure": "Clean Energy / Grid Infrastructure",
    "Renewable Fuels / Ethanol":          "Clean Energy / Renewable Fuels",
    "Energy - Decarbonization Technology":"Clean Energy / Decarbonization Tech",
    "Sustainability / Electronics Recycling": "Specialty Chemicals / Recycling & Sustainability",
    "Sustainability / Plastic Recycling": "Specialty Chemicals / Recycling & Sustainability",
    "Infrastructure / Energy & Renewables": "Clean Energy / Grid Infrastructure",

    # ── Oil & Gas ─────────────────────────────────────────────────────────
    "Energy / Oil & Gas Exploration & Production": "Oil & Gas / E&P",
    "Energy / Offshore E&P":              "Oil & Gas / E&P",
    "Energy / Oilfield Services":         "Oil & Gas / Oilfield Services",
    "Energy / Oilfield Services / Proppants": "Oil & Gas / Oilfield Services / Proppants",
    "Energy Services / Completion Fluids":"Oil & Gas / Oilfield Services / Completion Fluids",
    "Energy / Oil & Gas Drilling":        "Oil & Gas / Oilfield Services / Drilling",
    "Energy / Offshore Drilling":         "Oil & Gas / Offshore Drilling",
    "Energy - Offshore Drilling":         "Oil & Gas / Offshore Drilling",
    "Energy / Fracking & Completion":     "Oil & Gas / Oilfield Services / Pressure Pumping",
    "Energy / Oil Refining":              "Oil & Gas / Refining",
    "Natural Gas / LNG Infrastructure":   "Oil & Gas / LNG & Natural Gas",
    "Energy - Coal":                      "Oil & Gas / Coal",

    # ── Nuclear ───────────────────────────────────────────────────────────
    "Nuclear":                            "Nuclear / Uranium Mining",
    "Nuclear / Fuel Technology":          "Nuclear / Fuel Technology",

    # ── Power & Utilities ─────────────────────────────────────────────────
    "Electricity / Power Generation":     "Power & Utilities / Power Generation",
    "Electrcity/Power":                   "Power & Utilities / Power Generation",

    # ── Metals & Mining ───────────────────────────────────────────────────
    "Mining / Gold":                      "Metals & Mining / Gold",
    "Gold Mining":                        "Metals & Mining / Gold",
    "Mining - Gold":                      "Metals & Mining / Gold",
    "Precious Metals / Gold Mining":      "Metals & Mining / Gold",
    "Precious Metals / Gold & Silver Mining": "Metals & Mining / Gold",
    "Precious Metals / Silver & Gold Mining": "Metals & Mining / Silver",
    "Precious Metals / Silver Mining":    "Metals & Mining / Silver",
    "Mining / Silver":                    "Metals & Mining / Silver",
    "Precious Metals / Gold Royalty":     "Metals & Mining / Gold Royalty",
    "Precious Metals / Mining":           "Metals & Mining / Diversified",
    "Mining / Precious Metals":           "Metals & Mining / Diversified",
    "Mining / Copper":                    "Metals & Mining / Copper",
    "Metals / Copper":                    "Metals & Mining / Copper",
    "Mining / Lithium":                   "Metals & Mining / Lithium / Hard Rock",
    "Mining / Lithium - DLE":             "Metals & Mining / Lithium / Brine & DLE",
    "Metals / Lithium Extraction":        "Metals & Mining / Lithium / Brine & DLE",
    "Mining / Magnesium":                 "Metals & Mining / Magnesium",
    "Mining / Platinum Group Metals":     "Metals & Mining / Platinum Group",
    "Mining / PGM & Gold":                "Metals & Mining / Platinum Group",
    "Metals - Gold, Silver, Copper, Aluminum": "Metals & Mining / Diversified",
    "Strategic Minerals":                 "Metals & Mining / Rare Earth & Strategic",
    "Critical Minerals / Rare Earth Elements": "Metals & Mining / Rare Earth & Strategic",
    "Steel / Vertically Integrated":      "Metals & Mining / Steel",
    "Industrial Metals / Aluminum":       "Metals & Mining / Aluminum",

    # ── Space ─────────────────────────────────────────────────────────────
    "Space":                              "Space / Satellites & Communication",  # Default — overridden for specific tickers below
    "Space / Launches":                   "Space / Launch",
    "Space / Satellites & Communication": "Space / Satellites & Communication",
    "Space / Tourism":                    "Space / Tourism",
    "Infrastructure / Modular Space":     "Industrials / Modular Space",
    "Telecom / Satellite Services":       "Telecom / Satellite Services",

    # ── Defense & Aerospace ───────────────────────────────────────────────
    "Aerospace & Defense / Components":   "Defense & Aerospace / Defense Components",
    "Aerospace / Manufacturing":          "Defense & Aerospace / Commercial Aerospace / Manufacturing",
    "Aerospace / Aftermarket & MRO":      "Defense & Aerospace / Commercial Aerospace / MRO",
    "Aviation / Engine Maintenance & Leasing": "Defense & Aerospace / Commercial Aerospace / Engines",
    "Aviation / In-Flight Connectivity":  "Defense & Aerospace / Commercial Aerospace / In-Flight Connectivity",
    "Aviation / Electric & Hybrid":       "Defense & Aerospace / Regional & Electric Aviation",
    "Aviation / Regional Mobility":       "Defense & Aerospace / Regional & Electric Aviation",
    "Defense / Electronic Warfare":       "Defense & Aerospace / Electronic Warfare",
    "Defense / Ammunition":               "Defense & Aerospace / Ammunition",
    "Defense / Optical Systems":          "Defense & Aerospace / Optical Systems",
    "Drones":                             "Defense & Aerospace / Drones",

    # ── EV & Autonomous ──────────────────────────────────────────────────
    "EV & AV":                            "EV & Autonomous / EV Manufacturers",
    "EV / Charging Infrastructure":       "EV & Autonomous / Charging Infrastructure",
    "EV / Micromobility":                 "EV & Autonomous / Micromobility",
    "EV Infrastructure / V2G":            "EV & Autonomous / V2G Infrastructure",
    "LiDAR & AV Tech":                    "EV & Autonomous / ADAS & LiDAR",
    "Automotive / ADAS":                  "EV & Autonomous / ADAS & LiDAR",
    "Autonomous Vehicles / Software":     "EV & Autonomous / Autonomous Vehicles",

    # ── Automotive (Legacy) ───────────────────────────────────────────────
    "Automotive / Components":            "Automotive (Legacy) / Components",
    "Automotive / Aftermarket & MRO":     "Automotive (Legacy) / Aftermarket & MRO",
    "Automotive / Driveline & Powertrain":"Automotive (Legacy) / Driveline",
    "Automotive / Semiconductors":        "Automotive (Legacy) / Semiconductors",

    # ── Consumer ──────────────────────────────────────────────────────────
    "Consumer / Fast Casual Restaurants": "Consumer / Restaurants / Fast Casual",
    "Consumer / Casual Dining":           "Consumer / Restaurants / Casual Dining",
    "Consumer / Athletic Apparel":        "Consumer / Apparel & Footwear / Athletic",
    "Consumer / Footwear":                "Consumer / Apparel & Footwear / Athletic",
    "Consumer - Premium Footwear":        "Consumer / Apparel & Footwear / Premium & Luxury",
    "Consumer - Footwear Retail":         "Retail (Multi-Category) / Footwear Retail",
    "Consumer / Children's Apparel":      "Consumer / Apparel & Footwear / Children's",
    "Consumer / Cosmetics":               "Consumer / Cosmetics & Personal Care",
    "Consumer / Eyewear":                 "Consumer / Eyewear",
    "Consumer / Protein & Nutrition":     "Consumer / Protein & Nutrition",
    "Consumer / RV & Outdoor Lifestyle":  "Consumer / RV & Outdoor Lifestyle",
    "Consumer / Cannabinoids":            "Consumer / Cannabinoids",
    "Consumer Goods / Health & Wellness": "Consumer / Health & Wellness",

    # ── Retail (Multi-Category) ───────────────────────────────────────────
    "Retail / Specialty Apparel":         "Retail (Multi-Category) / Specialty Apparel",
    "Retail / Home Goods":                "Retail (Multi-Category) / Home Goods",
    "Retail / Discount Grocery":          "Retail (Multi-Category) / Discount Grocery",
    "Retail / Discount Specialty":        "Retail (Multi-Category) / Discount Specialty",
    "Retail / Sporting Goods & Outdoor":  "Retail (Multi-Category) / Sporting Goods",

    # ── Real Estate ───────────────────────────────────────────────────────
    "Real Estate / Diversified REIT":     "Real Estate / Diversified REIT",
    "Real Estate / Office & Residential": "Real Estate / Office & Commercial REIT",
    "Real Estate - Residential":          "Real Estate / Residential REIT",
    "Real Estate / Tech-Enabled Brokerage": "Real Estate / Tech-Enabled Brokerage",
    "Real Estate / iBuying":              "Real Estate / iBuying & PropTech",

    # ── Industrials ───────────────────────────────────────────────────────
    "Industrial / Heavy Machinery Components": "Industrials / Heavy Machinery & Components",
    "Industrial / Additive Manufacturing":"Industrials / Additive Manufacturing",
    "Industrial / Testing & Certification":"Industrials / Testing & Certification",
    "Industrial / Distribution Technology": "Industrials / Distribution Technology",
    "Electronic Manufacturing Services (EMS)": "Industrials / Electronic Manufacturing",
    "Building Materials / Distribution":  "Industrials / Building Materials",
    "Building Materials / Wood Products": "Industrials / Wood Products",
    "Building Materials / Flooring":      "Industrials / Flooring",
    "Building Materials / PVC":           "Industrials / PVC",

    # ── Logistics ─────────────────────────────────────────────────────────
    "Logistics / Maritime Shipping":      "Logistics / Maritime Shipping",
    "Logistics / Freight Brokerage":      "Logistics / Freight Brokerage & Trucking",
    "Logistics / Container Leasing":      "Logistics / Container Leasing",

    # ── Telecom ───────────────────────────────────────────────────────────
    "Telecom / 5G Infrastructure":        "Telecom / 5G Infrastructure",
    "Telecom / Fiber Infrastructure":     "Telecom / Fiber",
    "Telecommunications / Argentina":     "Telecom / Argentina",
    "Telecommunications / Latin America": "Telecom / Latin America",

    # ── Travel & Leisure ──────────────────────────────────────────────────
    "Travel & Leisure / Cruise Lines":    "Travel & Leisure / Cruise Lines",
    "Travel & Leisure / Online Travel Agencies": "Travel & Leisure / Online Travel",
    "Travel & Leisure / Short-term Rentals": "Travel & Leisure / Short-term Rentals",
    "Transportation / Car Rental":        "Travel & Leisure / Car Rental",
    "Airlines / Major Carriers":          "Travel & Leisure / Airlines / Major",
    "Airlines / Low-Cost Carriers":       "Travel & Leisure / Airlines / Low-Cost",
    "Airlines / Ultra-Low-Cost Carriers": "Travel & Leisure / Airlines / Ultra-Low-Cost",
    "Entertainment / Themed Attractions": "Travel & Leisure / Themed Attractions",
    "Entertainment / Theme Parks":        "Travel & Leisure / Theme Parks",
    "Entertainment / Live Events":        "Travel & Leisure / Live Events",

    # ── Quantum & Robotics ────────────────────────────────────────────────
    "Quantum Computing":                  "Quantum Computing",

    # ── Agriculture ───────────────────────────────────────────────────────
    "Agriculture / Crop Protection":      "Agriculture / Crop Protection",
    "Agriculture / Fertilizers":          "Agriculture / Fertilizers",
    "Agriculture / Processing & Trading": "Agriculture / Processing & Trading",
    "Agriculture / Alternative Protein":  "Agriculture / Alternative Protein",

    # ── Cannabis ──────────────────────────────────────────────────────────
    "Cannabis":                           "Cannabis / Cultivation",
    "Cannabis / Cultivation":             "Cannabis / Cultivation",
    "Cannabis / Cultivation & Brands":    "Cannabis / Cultivation",
    "Cannabis / Multi-State Operators & Brands": "Cannabis / Multi-State Operators & Brands",

    # ── Specialty Chemicals ───────────────────────────────────────────────
    "Chemicals / Specialty Chemicals":    "Specialty Chemicals / Specialty Materials",
    "Chemicals / Specialty Materials":    "Specialty Chemicals / Specialty Materials",
    "Chemicals / Specialized Materials":  "Specialty Chemicals / Specialty Materials",
    "Chemicals / Chlor-Alkali":           "Specialty Chemicals / Chlor-Alkali",
    "Specialty Chemicals / Titanium Dioxide": "Specialty Chemicals / Titanium Dioxide",

    # ── Gig Economy ──────────────────────────────────────────────────────
    "Gig Economy / Ride-Hailing":         "Gig Economy / Ride-Hailing",
    "Gig Economy / Delivery Platforms":   "Gig Economy / Delivery",
    "Gig Economy / Freelance Platforms":  "Gig Economy / Freelance Platforms",

    # ── Security/Surveillance ─────────────────────────────────────────────
    "Private Prisons & Detention":        "Cybersecurity / Surveillance & IoT",

    # ── Drop / Singleton ──────────────────────────────────────────────────
    "Individual Episodic Pivots / Singletons": "Singleton",
    "Uncategorized":                      "Singleton",
    "Meme Stocks":                        "Singleton",
}


# ─────────────────────────────────────────────────────────────────────────────
# Per-ticker overrides — explicit re-tags for Singletons, Uncategorized, Memes,
# and other special cases where the old theme was ambiguous or wrong.
# ─────────────────────────────────────────────────────────────────────────────
TICKER_OVERRIDES: dict[str, list[str]] = {
    # ── Space disambiguation (default "Space" → Satellites; override these) ─
    "RKLB":   ["Space / Launch"],
    "ASTC":   ["Space / Satellites & Communication"],
    "ASTS":   ["Space / Satellites & Communication"],
    "BKSY":   ["Space / Imaging & Earth Observation"],
    "PL":     ["Space / Imaging & Earth Observation"],
    "GSAT":   ["Space / Satellites & Communication"],
    "IRDM":   ["Space / Satellites & Communication"],
    "GILT":   ["Space / Satellites & Communication"],
    "LUNR":   ["Space / Infrastructure"],
    "RDW":    ["Space / Infrastructure"],
    "SATL":   ["Space / Satellites & Communication"],
    "SATS":   ["Space / Satellites & Communication"],
    "SIDU":   ["Space / Satellites & Communication"],
    "MNTS":   ["Space / Launch", "Space / Satellites & Communication"],
    "SPIR":   ["Space / Satellites & Communication"],
    "VOYG":   ["Space / Infrastructure"],
    "VSAT":   ["Space / Satellites & Communication"],
    "SPCE":   ["Space / Tourism"],
    "EMBJ":   ["Space / Satellites & Communication"],

    # ── Singletons / Uncategorized / Memes — meaningful re-classification ──
    "ADI":    ["Semiconductors / Power & Discrete"],
    "AGL":    ["Healthcare / Managed Care"],
    "ANNA":   ["Oil & Gas / E&P"],
    "ASAN":   ["Software & Internet / SaaS & Vertical"],
    "AXGN":   ["MedTech / Surgical & Devices"],
    "B":      ["Industrials / Heavy Machinery & Components"],
    "BAX":    ["MedTech / Surgical & Devices"],
    "BBGI":   ["Software & Internet / Streaming"],
    "BBNX":   ["MedTech / Diabetes"],
    "BBWI":   ["Consumer / Cosmetics & Personal Care"],
    "BGS":    ["Consumer / Food & Beverage"],
    "BHC":    ["Healthcare / Specialty Pharmaceuticals"],
    "BLSH":   ["Fintech & Crypto / Crypto / Exchanges & Brokers"],
    "BZAI":   ["AI / Data Center / Chips & Processors"],
    "CECO":   ["Industrials / Engineering & Construction"],
    "CMC":    ["Metals & Mining / Steel"],
    "DVA":    ["Healthcare / Hospitals & Facilities"],
    "DXF":    ["Fintech & Crypto / Specialty Lending"],
    "DXST":   ["Singleton"],
    "EFOI":   ["Singleton"],
    "ERII":   ["Industrials / Heavy Machinery & Components"],
    "EWBC":   ["Fintech & Crypto / Consumer Finance"],
    "FCHL":   ["Education / Higher Education"],
    "FLG":    ["Fintech & Crypto / Mortgage Lending"],
    "FLS":    ["Industrials / Heavy Machinery & Components"],
    "FRPT":   ["Consumer / Food & Beverage"],
    "FUSE":   ["Singleton"],
    "GITS":   ["Singleton"],
    "GPK":    ["Industrials / Building Materials"],
    "GPRO":   ["Consumer / Outdoor & Recreation"],
    "HNGE":   ["Singleton"],
    "HPQ":    ["AI / Hardware / AR Glasses"],
    "INTR":   ["Fintech & Crypto / Consumer Finance", "Geographic / Latin America"],
    "IP":     ["Industrials / Building Materials"],
    "ISPC":   ["Healthcare / CRO & Clinical Services"],
    "JDZG":   ["Singleton"],
    "JHX":    ["Industrials / Building Materials"],
    "KNRX":   ["Singleton"],
    "KODK":   ["Specialty Chemicals / Specialty Materials"],
    "LIMN":   ["Singleton"],
    "LTH":    ["Consumer / Health & Wellness"],
    "LZ":     ["Software & Internet / SaaS & Vertical"],
    "MANE":   ["Singleton"],
    "MCW":    ["Consumer / Outdoor & Recreation"],
    "MNST":   ["Consumer / Food & Beverage"],
    "MOH":    ["Healthcare / Managed Care"],
    "MRLN":   ["Singleton"],
    "MWH":    ["Clean Energy / Solar / C&I Install"],
    "MYSE":   ["Singleton"],
    "NPT":    ["Singleton"],
    "NVRI":   ["Industrials / Engineering & Construction"],
    "NYT":    ["Software & Internet / Streaming"],
    "OFRM":   ["Singleton"],
    "ONEG":   ["Industrials / Engineering & Construction"],
    "ONFO":   ["Software & Internet / E-commerce"],
    "OTF":    ["Singleton"],
    "PIII":   ["Healthcare / Managed Care"],
    "PM":     ["Consumer / Food & Beverage"],
    "PRMB":   ["Consumer / Food & Beverage"],
    "PSKY":   ["Software & Internet / Streaming"],
    "PTRN":   ["Singleton"],
    "PURR":   ["Singleton"],
    "RCI":    ["Telecom / Wireless Carriers"],
    "RECT":   ["Singleton"],
    "RLJ":    ["Real Estate / Diversified REIT"],
    "RUBI":   ["Singleton"],
    "SABR":   ["Software & Internet / Travel Tech"],
    "SBS":    ["Power & Utilities / Diversified Utilities", "Geographic / Latin America"],
    "SCI":    ["Singleton"],
    "SGP":    ["Singleton"],
    "SKK":    ["Industrials / Engineering & Construction"],
    "SKM":    ["Telecom / Wireless Carriers"],
    "SKYQ":   ["Oil & Gas / Refining"],
    "SNX":    ["Industrials / Distribution Technology"],
    "SON":    ["Industrials / Building Materials"],
    "SONO":   ["Consumer / Home Goods & Furnishings"],
    "STAK":   ["Singleton"],
    "SUNB":   ["Industrials / Heavy Machinery & Components"],
    "SW":     ["Industrials / Building Materials"],
    "SWK":    ["Consumer / Home Goods & Furnishings"],
    "SWMR":   ["Singleton"],
    "TDAY":   ["Singleton"],
    "UPS":    ["Logistics / Freight Brokerage & Trucking"],
    "USFD":   ["Logistics / Freight Brokerage & Trucking"],
    "VALE":   ["Metals & Mining / Diversified"],
    "VGNT":   ["Singleton"],
    "VHUB":   ["Singleton"],
    "VMC":    ["Industrials / Building Materials"],
    "WH":     ["Travel & Leisure / Online Travel"],
    "WM":     ["Industrials / Engineering & Construction"],
    "WOOF":   ["Consumer / Food & Beverage"],
    "XHLD":   ["Singleton"],
    "XWEL":   ["Singleton"],
    "XYZ":    ["Fintech & Crypto / Payments"],
    "ZTO":    ["Logistics / Freight Brokerage & Trucking"],
    "ZWS":    ["Industrials / Heavy Machinery & Components"],

    # AAPL — diversified, keep as singleton (too unique for clean L1 fit)
    "AAPL":   ["Singleton"],

    # Uncategorized
    "ARQQ":   ["Quantum Computing"],
    "ASPN":   ["EV & Autonomous / EV Components"],
    "BTM":    ["Fintech & Crypto / Crypto / Infrastructure"],
    "CG":     ["Fintech & Crypto / Private Equity Proxy"],
    "CTNT":   ["Logistics / Supply Chain Tech"],
    "DAVE":   ["Fintech & Crypto / Consumer Finance"],
    "ETHA":   ["Fintech & Crypto / Crypto / Bitcoin Proxy"],
    "FUTU":   ["Fintech & Crypto / Wealth & Asset Management"],
    "GWH":    ["Clean Energy / Batteries & Storage"],
    "LAZR":   ["EV & Autonomous / ADAS & LiDAR"],
    "META":   ["Software & Internet / Social & Communication", "AI / Software & Analytics"],
    "NEGG":   ["Software & Internet / E-commerce"],
    "POWL":   ["AI / Data Center / Power & Cooling"],
    "QSI":    ["Healthcare / Diagnostics & Labs"],
    "TLS":    ["Cybersecurity / Identity"],
    "TMDX":   ["MedTech / Surgical & Devices"],
    "WGS":    ["Healthcare / Diagnostics & Labs"],

    # Meme stocks → real classifications
    "AMC":    ["Travel & Leisure / Theme Parks"],
    "CVNA":   ["Retail (Multi-Category) / Auto Marketplace"],
    "GME":    ["Retail (Multi-Category) / Specialty Apparel"],
    "KSS":    ["Retail (Multi-Category) / Big-Box"],
    "OPEN":   ["Real Estate / iBuying & PropTech"],
    "PTON":   ["Consumer / Health & Wellness"],
    "WNW":    ["Singleton"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Multi-theme augmentation — add secondary L1 paths for diversified tickers.
# Applied AFTER the main mapping; max 3 total paths per ticker.
# ─────────────────────────────────────────────────────────────────────────────
MULTI_THEME_ADDITIONS: dict[str, list[str]] = {
    # AMZN, GOOGL: cloud + core business
    "AMZN":   ["Software & Internet / E-commerce"],
    "GOOGL":  ["Software & Internet / Ads & Marketing Tech"],
    "GOOG":   ["Software & Internet / Ads & Marketing Tech"],
    "MSFT":   ["Software & Internet / SaaS & Vertical"],
    "NVDA":   ["AI / Data Center / Chips & Processors"],  # already AI but emphasize
}


# ─────────────────────────────────────────────────────────────────────────────
# Taxonomy loader & validator (mirror of theme_taxonomy.py logic)
# ─────────────────────────────────────────────────────────────────────────────
def load_taxonomy(path: Path = None) -> dict:
    path = path or ROOT / "config" / "theme_taxonomy.yaml"
    with open(path) as f:
        return yaml.safe_load(f).get("themes", {})


def _children(node) -> dict:
    """Normalize a node's children to a dict {name: subtree}."""
    if not isinstance(node, dict):
        return {}
    raw = node.get("children", {})
    if isinstance(raw, list):
        return {name: {} for name in raw}
    if isinstance(raw, dict):
        return raw
    return {}


def validate_path(path: str, taxonomy: dict) -> bool:
    parts = [p.strip() for p in path.split(" / ")]
    if not parts:
        return False
    l1 = parts[0]
    if l1 not in taxonomy:
        return False
    if len(parts) == 1:
        return True
    l2 = parts[1]
    l2_children = _children(taxonomy[l1])
    if l2 not in l2_children:
        return False
    if len(parts) == 2:
        return True
    l3 = parts[2]
    l3_children = _children(l2_children[l2])
    return l3 in l3_children


# ─────────────────────────────────────────────────────────────────────────────
# Migration
# ─────────────────────────────────────────────────────────────────────────────
def migrate() -> tuple[dict, list[str]]:
    """Run the migration and return (new_themes, warnings)."""
    snapshot = ROOT / "data" / "ticker_themes.pre_migration.json"
    with open(snapshot) as f:
        old = json.load(f)

    taxonomy = load_taxonomy()
    new: dict[str, list[str]] = {}
    warnings: list[str] = []
    unmapped: Counter = Counter()

    for ticker, old_themes in sorted(old.items()):
        if ticker in TICKER_OVERRIDES:
            new[ticker] = list(TICKER_OVERRIDES[ticker])
            continue

        new_paths: list[str] = []
        for theme in old_themes:
            if theme in OLD_TO_NEW:
                new_path = OLD_TO_NEW[theme]
                if new_path and new_path not in new_paths:
                    new_paths.append(new_path)
            else:
                unmapped[theme] += 1
                warnings.append(f"UNMAPPED theme '{theme}' on ticker {ticker}")

        if not new_paths:
            new_paths = ["Singleton"]

        if ticker in MULTI_THEME_ADDITIONS:
            for addition in MULTI_THEME_ADDITIONS[ticker]:
                if addition not in new_paths and len(new_paths) < 3:
                    new_paths.append(addition)

        # Cap at 3 paths
        new_paths = new_paths[:3]

        # Validate each path
        for p in new_paths:
            if not validate_path(p, taxonomy):
                warnings.append(f"INVALID PATH '{p}' on ticker {ticker}")

        new[ticker] = new_paths

    if unmapped:
        warnings.append("")
        warnings.append("=== Unmapped themes (count, theme) ===")
        for theme, n in unmapped.most_common():
            warnings.append(f"  {n:4d}  {theme}")

    return new, warnings


def main():
    new, warnings = migrate()

    out = ROOT / "data" / "ticker_themes.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(new, f, indent=2, sort_keys=True)
    print(f"Wrote {len(new)} tickers -> {out}")

    # Stats
    path_counts = Counter(len(v) for v in new.values())
    print(f"\nPaths per ticker: {dict(sorted(path_counts.items()))}")

    l1_counts: Counter = Counter()
    leaf_counts: Counter = Counter()
    for paths in new.values():
        for p in paths:
            l1_counts[p.split(" / ")[0]] += 1
            leaf_counts[p] += 1
    print(f"\nL1 distribution ({len(l1_counts)} L1s):")
    for l1, n in l1_counts.most_common():
        print(f"  {n:4d}  {l1}")
    print(f"\nTotal leaf paths: {len(leaf_counts)}")

    # Warnings
    if warnings:
        log = ROOT / "reports" / "migration_warnings.log"
        log.parent.mkdir(exist_ok=True)
        with open(log, "w", encoding="utf-8") as f:
            f.write("\n".join(warnings))
        invalid = [w for w in warnings if "INVALID" in w or "UNMAPPED" in w]
        if invalid:
            print(f"\n⚠ {len(invalid)} warnings → {log}")
            for w in invalid[:10]:
                print(f"   {w}")


if __name__ == "__main__":
    main()
