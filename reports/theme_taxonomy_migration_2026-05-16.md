# Theme Taxonomy Migration — 2026-05-16

Migration from flat LLM-emitted themes (consolidated at display time by `config/theme_groups.yaml`) to a canonical 3-level hierarchy (`config/theme_taxonomy.yaml`). Every ticker is now stored as one to three slash-delimited paths whose L1 is a coherent trading narrative.

---

## Summary statistics

### Before (pre-migration)

- Tickers: **1655**
- Distinct theme strings: **324**
- Total path assignments: **1975**
- Paths per ticker:
  - 1 path(s): 1337 tickers
  - 2 path(s): 316 tickers
  - 3 path(s): 2 tickers
- Path depth distribution:
  - L1: 524 assignments
  - L1/L2: 1447 assignments
  - L1/L2/L3: 4 assignments

### After (canonical taxonomy)

- Tickers: **1655**
- Distinct theme strings: **252**
- Total path assignments: **1952**
- Paths per ticker:
  - 1 path(s): 1360 tickers
  - 2 path(s): 293 tickers
  - 3 path(s): 2 tickers
- Path depth distribution:
  - L1: 37 assignments
  - L1/L2: 1607 assignments
  - L1/L2/L3: 308 assignments

### L1 narrative distribution (new taxonomy)

| L1 | Tickers |
|----|--------:|
| Biotech | 302 |
| AI | 207 |
| Fintech & Crypto | 173 |
| Software & Internet | 168 |
| Oil & Gas | 124 |
| Metals & Mining | 105 |
| Consumer | 89 |
| Healthcare | 82 |
| Clean Energy | 63 |
| Real Estate | 59 |
| Industrials | 54 |
| Retail (Multi-Category) | 45 |
| MedTech | 40 |
| Cybersecurity | 37 |
| Defense & Aerospace | 36 |
| Power & Utilities | 36 |
| EV & Autonomous | 36 |
| Travel & Leisure | 32 |
| Logistics | 30 |
| Singleton | 29 |
| Telecom | 28 |
| Specialty Chemicals | 24 |
| Semiconductors | 24 |
| Automotive (Legacy) | 22 |
| Nuclear | 21 |
| Space | 21 |
| Agriculture | 16 |
| Education | 13 |
| Cannabis | 8 |
| Geographic | 8 |
| Quantum Computing | 8 |
| Robotics | 8 |
| Gig Economy | 4 |

---

### Spotlight — the BE/PUMP fix

Before the migration these tickers all shared the same 'Energy' node on VARS Viz. After the migration, fuel-cell and oilfield-services tickers belong to different L1 narratives ('Clean Energy' vs 'Oil & Gas') and connect to different hub nodes in the Cytoscape graph.

| Cluster | Ticker | Before | After |
|---------|--------|--------|-------|
| Clean Energy (Fuel Cell) | BE | Fuel Cell | Clean Energy / Fuel Cell & Hydrogen |
| Clean Energy (Fuel Cell) | FCEL | Fuel Cell | Clean Energy / Fuel Cell & Hydrogen |
| Clean Energy (Fuel Cell) | BLDP | Fuel Cell | Clean Energy / Fuel Cell & Hydrogen |
| Clean Energy (Fuel Cell) | PLUG | Fuel Cell | Clean Energy / Fuel Cell & Hydrogen |
| Clean Energy (Batteries) | AMPX | Batteries | Clean Energy / Batteries & Storage |
| Clean Energy (Batteries) | MVST | Batteries | Clean Energy / Batteries & Storage |
| Clean Energy (Batteries) | QS | Batteries | Clean Energy / Batteries & Storage |
| Oil & Gas (Services) | PUMP | Energy / Fracking & Completion | Oil & Gas / Oilfield Services / Pressure Pumping |
| Oil & Gas (Services) | HAL | Energy / Oilfield Services | Oil & Gas / Oilfield Services |
| Oil & Gas (Services) | SLB | Energy / Oilfield Services | Oil & Gas / Oilfield Services |
| Oil & Gas (Services) | NOV | Energy / Oilfield Services | Oil & Gas / Oilfield Services |
| Oil & Gas (E&P) | XOM | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| Oil & Gas (E&P) | CVX | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| Oil & Gas (E&P) | FANG | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| Oil & Gas (E&P) | EOG | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| Space cluster | RKLB | Space | Space / Launch |
| Space cluster | PL | Space | Space / Imaging & Earth Observation |
| Space cluster | ASTS | Space | Space / Satellites & Communication |
| Space cluster | IRDM | Space / Satellites & Communication | Space / Satellites & Communication |
| AI Data Center / Memory | SNDK | AI - Memory & Storage | AI / Data Center / Memory |
| AI Data Center / Memory | MU | AI - Memory & Storage | AI / Data Center / Memory |
| AI Data Center / Memory | STX | AI - Memory & Storage | AI / Data Center / Memory |
| AI Data Center / Memory | WDC | AI - Memory & Storage | AI / Data Center / Memory |

---

### Granularity gains — tickers that escaped the catch-all buckets

_93 tickers gained meaningful themes_

| Ticker | Before | After |
|--------|--------|-------|
| ADI | Individual Episodic Pivots / Singletons | Semiconductors / Power & Discrete |
| AGL | Individual Episodic Pivots / Singletons | Healthcare / Managed Care |
| AMC | Meme Stocks | Travel & Leisure / Theme Parks |
| ANNA | Individual Episodic Pivots / Singletons | Oil & Gas / E&P |
| ARQQ | Uncategorized | Quantum Computing |
| ASAN | Individual Episodic Pivots / Singletons | Software & Internet / SaaS & Vertical |
| ASPN | Uncategorized | EV & Autonomous / EV Components |
| ASTC | Individual Episodic Pivots / Singletons | Space / Satellites & Communication |
| AXGN | Individual Episodic Pivots / Singletons | MedTech / Surgical & Devices |
| B | Individual Episodic Pivots / Singletons | Industrials / Heavy Machinery & Components |
| BAX | Individual Episodic Pivots / Singletons | MedTech / Surgical & Devices |
| BBGI | Individual Episodic Pivots / Singletons | Software & Internet / Streaming |
| BBNX | Individual Episodic Pivots / Singletons | MedTech / Diabetes |
| BBWI | Individual Episodic Pivots / Singletons | Consumer / Cosmetics & Personal Care |
| BGS | Individual Episodic Pivots / Singletons | Consumer / Food & Beverage |
| BHC | Individual Episodic Pivots / Singletons | Healthcare / Specialty Pharmaceuticals |
| BLSH | Individual Episodic Pivots / Singletons | Fintech & Crypto / Crypto / Exchanges & Brokers |
| BTM | Uncategorized | Fintech & Crypto / Crypto / Infrastructure |
| BZAI | Individual Episodic Pivots / Singletons | AI / Data Center / Chips & Processors |
| CECO | Individual Episodic Pivots / Singletons | Industrials / Engineering & Construction |
| CG | Uncategorized | Fintech & Crypto / Private Equity Proxy |
| CMC | Individual Episodic Pivots / Singletons | Metals & Mining / Steel |
| CTNT | Uncategorized | Logistics / Supply Chain Tech |
| CVNA | Meme Stocks | Retail (Multi-Category) / Auto Marketplace |
| DAVE | Uncategorized | Fintech & Crypto / Consumer Finance |
| DVA | Individual Episodic Pivots / Singletons | Healthcare / Hospitals & Facilities |
| DXF | Individual Episodic Pivots / Singletons | Fintech & Crypto / Specialty Lending |
| ERII | Individual Episodic Pivots / Singletons | Industrials / Heavy Machinery & Components |
| ETHA | Uncategorized | Fintech & Crypto / Crypto / Bitcoin Proxy |
| EWBC | Individual Episodic Pivots / Singletons | Fintech & Crypto / Consumer Finance |
| FCHL | Individual Episodic Pivots / Singletons | Education / Higher Education |
| FLG | Individual Episodic Pivots / Singletons | Fintech & Crypto / Mortgage Lending |
| FLS | Individual Episodic Pivots / Singletons | Industrials / Heavy Machinery & Components |
| FRPT | Individual Episodic Pivots / Singletons | Consumer / Food & Beverage |
| FUTU | Uncategorized | Fintech & Crypto / Wealth & Asset Management |
| GME | Meme Stocks | Retail (Multi-Category) / Specialty Apparel |
| GPK | Individual Episodic Pivots / Singletons | Industrials / Building Materials |
| GPRO | Individual Episodic Pivots / Singletons | Consumer / Outdoor & Recreation |
| GWH | Uncategorized | Clean Energy / Batteries & Storage |
| HPQ | Individual Episodic Pivots / Singletons | AI / Hardware / AR Glasses |

_…and 53 more_


---

### L1 reshuffle — tickers whose narrative moved (capped at 60)

| Ticker | Before | After |
|--------|--------|-------|
| AA | Industrial Metals / Aluminum | Metals & Mining / Aluminum |
| AAL | Airlines / Major Carriers | Travel & Leisure / Airlines / Major |
| AAOI | AI - Optics | AI / Data Center / Optics |
| AAON | AI - Infra / Power/Cooling | AI / Data Center / Power & Cooling |
| AAP | Automotive / Aftermarket & MRO | Automotive (Legacy) / Aftermarket & MRO |
| AAPL | Individual Episodic Pivots / Singletons | Singleton |
| ABAT | Batteries | Clean Energy / Batteries & Storage |
| ABCL | AI - Biotech & Drug Discovery | AI / Drug Discovery |
| ACDC | Energy / Oilfield Services / Proppants | Oil & Gas / Oilfield Services / Proppants |
| ACHR | Drones | Defense & Aerospace / Drones |
| ACLS | AI - Semiconductor Processing | AI / Semiconductor Equipment |
| ACM | Infrastructure / Energy & Renewables | Clean Energy / Grid Infrastructure |
| ACMR | AI - Semiconductor Processing | AI / Semiconductor Equipment |
| ACVA | E-commerce / Automotive Marketplace | Retail (Multi-Category) / Auto Marketplace |
| ADEA | Semiconductor / IP Licensing | Semiconductors / IP Licensing |
| ADI | Individual Episodic Pivots / Singletons | Semiconductors / Power & Discrete |
| ADNT | Automotive / Components | Automotive (Legacy) / Components |
| AEHL | AI - Data Center & Cloud | AI / Data Center / Cloud & Hyperscalers |
| AEHR | AI - Semiconductor Testing | AI / Semiconductor Equipment |
| AEP | Electricity / Power Generation | Power & Utilities / Power Generation |
| AESI | Energy / Oilfield Services / Proppants | Oil & Gas / Oilfield Services / Proppants |
| AFRM | Fintech & Digital Payments | Fintech & Crypto / Payments |
| AG | Metals - Gold, Silver, Copper, Aluminum | Metals & Mining / Diversified |
| AGAE | Gaming | Software & Internet / Gaming |
| AGL | Individual Episodic Pivots / Singletons | Healthcare / Managed Care |
| AGPU | AI - Biotech & Drug Discovery | AI / Drug Discovery |
| AI | AI - Software Services | AI / Software & Analytics |
| AIP | Semiconductor / IP Licensing | Semiconductors / IP Licensing |
| AIXI | AI - Conversational Avatars | AI / Hardware / Avatars |
| AKTS | Connectivity / RF Filters | Telecom / RF Filters |
| ALAB | AI - Data Center Components | AI / Data Center / Components |
| ALBT | Real Estate - Residential | Real Estate / Residential REIT |
| ALGM | AI - Data Center Power | AI / Data Center / Power & Cooling |
| ALH | Human Capital Management / Benefits Tech | Software & Internet / SaaS & Vertical |
| ALIT | Human Capital Management / Benefits Tech | Business Process Outsourcing (BPO) | Software & Internet / SaaS & Vertical | Software & Internet / BPO & IT Services |
| ALK | Airlines / Major Carriers | Travel & Leisure / Airlines / Major |
| ALM | Smart Home / IoT Security | Cybersecurity / Surveillance & IoT |
| ALMU | AI - Optoelectronics | AI - Infra / Connectivity | AI / Data Center / Optics | AI / Data Center / Networking & Connectivity |
| ALOY | Critical Minerals / Rare Earth Elements | Metals & Mining / Rare Earth & Strategic |
| ALTO | Renewable Fuels / Ethanol | Clean Energy / Renewable Fuels |
| ALTS | Solar / Commercial & Industrial | Energy Storage / Grid Infrastructure | Clean Energy / Solar / C&I Install | Clean Energy / Grid Infrastructure |
| AM | Energy / Oilfield Services | Oil & Gas / Oilfield Services |
| AMAT | AI - Semiconductor Processing | AI / Semiconductor Equipment |
| AMBA | AI - Infra / Core Chips | AI - Processors | AI / Data Center / Chips & Processors |
| AMBQ | AI - Infra / Core Chips | AI / Data Center / Chips & Processors |
| AMC | Meme Stocks | Travel & Leisure / Theme Parks |
| AMD | AI - Processors | AI / Data Center / Chips & Processors |
| AMKR | AI - Semiconductor Testing | AI / Semiconductor Equipment |
| AMPX | Batteries | Clean Energy / Batteries & Storage |
| AMSC | Electrcity/Power | Power & Utilities / Power Generation |
| AMX | Telecommunications / Latin America | Telecom / Latin America |
| AMZN | E-commerce and Digital Retail | AI - Data Center & Cloud Services | Software & Internet / E-commerce | AI / Data Center / Cloud & Hyperscalers |
| ANET | AI - Data Center & Cloud | AI - Infra / Connectivity | AI / Data Center / Cloud & Hyperscalers | AI / Data Center / Networking & Connectivity |
| ANGI | Home Services Marketplace | Real Estate / Home Services Marketplace |
| ANL | Healthcare / Latin America | Geographic / Latin America |
| ANNA | Individual Episodic Pivots / Singletons | Oil & Gas / E&P |
| AOSL | AI - Data Center Power | AI / Data Center / Power & Cooling |
| APA | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| APD | Energy - Decarbonization Technology | Chemicals / Specialty Materials | Clean Energy / Decarbonization Tech | Specialty Chemicals / Specialty Materials |
| APG | Security / Surveillance Technology | Cybersecurity / Surveillance & IoT |

_…and 750 more_


---

### 50 random ticker diffs

| Ticker | Before | After |
|--------|--------|-------|
| SLDP | Batteries | Clean Energy / Batteries & Storage |
| BKSY | Space | Space / Imaging & Earth Observation |
| AGNC | Financial Services / Mortgage Lending | Fintech & Crypto / Mortgage Lending |
| VEEV | Healthcare / Data & Analytics | Healthcare / Data & Analytics |
| FABC | EV / Micromobility | EV & Autonomous / Micromobility |
| EL | Consumer / Cosmetics | Consumer / Cosmetics & Personal Care |
| DNA | Biotechnology / Oncology | Life Sciences / Genomics | Biotech / Oncology / General | Biotech / Genomics & Diagnostics Platforms |
| CARR | AI - Infra / Power/Cooling | AI / Data Center / Power & Cooling |
| USEG | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| BFRG | AI - Biotech & Drug Discovery | AI / Drug Discovery |
| SWKS | Connectivity / RF Filters | AI - Infra / Core Chips | Telecom / RF Filters | AI / Data Center / Chips & Processors |
| VCYT | Healthcare / Diagnostics | Biotech / Precision Oncology | Healthcare / Diagnostics & Labs | Biotech / Oncology / Precision |
| PMAX | Business Process Outsourcing (BPO) | Software & Internet / BPO & IT Services |
| AVR | Biotech / Oncology | Biotech / Oncology / General |
| RMAX | Real Estate / Tech-Enabled Brokerage | Real Estate / Tech-Enabled Brokerage |
| LXEO | Biotechnology / Gene Therapy | Healthcare / Cardiovascular | Biotech / Cell & Gene Therapy | Healthcare / Specialty Pharmaceuticals |
| AKAM | AI - Data Center & Cloud Services | Cybersecurity | AI / Data Center / Cloud & Hyperscalers | Cybersecurity / Generalist |
| AIIO | EV & AV | EV & Autonomous / EV Manufacturers |
| BATL | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| DGX | Healthcare / Diagnostics | Healthcare / Diagnostics & Labs |
| DV | Ads Tech & Marketing | Software & Internet / Ads & Marketing Tech |
| ONFO | Individual Episodic Pivots / Singletons | Software & Internet / E-commerce |
| RUN | Solar | Clean Energy / Solar |
| AGRZ | Agriculture / Processing & Trading | Agriculture / Processing & Trading |
| PURR | Individual Episodic Pivots / Singletons | Singleton |
| CTRA | Energy / Oil & Gas Exploration & Production | Oil & Gas / E&P |
| TTMI | AI - Semiconductor Processing | AI / Semiconductor Equipment |
| SNDR | Logistics / Freight Brokerage | Logistics / Freight Brokerage & Trucking |
| TOST | Fintech / Digital Payments | Fintech & Crypto / Payments |
| LULU | Consumer / Athletic Apparel | Consumer / Apparel & Footwear / Athletic |
| DINO | Energy / Oil Refining | Oil & Gas / Refining |
| MRAM | AI - Memory & Storage | AI / Data Center / Memory |
| RLAY | AI - Drug Discovery | Biotech / Precision Medicine | AI / Drug Discovery | Biotech / Drug Discovery Platform |
| FCEL | Fuel Cell | Clean Energy / Fuel Cell & Hydrogen |
| ABUS | Biotechnology / Oncology | Biotech / Oncology / General |
| VRT | AI - Data Center Power | AI / Data Center / Power & Cooling |
| ZS | Cybersecurity | Cybersecurity / Generalist |
| CHYM | Fintech & Digital Payments | Fintech & Crypto / Payments |
| TMC | Strategic Minerals | Metals & Mining / Rare Earth & Strategic |
| LXU | Agriculture / Fertilizers | Agriculture / Fertilizers |
| HOOD | Cryptocurrency | Fintech & Crypto / Crypto / Generalist |
| CGC | Cannabis | Cannabis / Cultivation |
| DDOG | Cybersecurity | Cybersecurity / Generalist |
| VSTS | Business Services / Uniform Rental & Workplace Supplies | Industrials / Workplace Services & Uniforms |
| HLF | Consumer Goods / Health & Wellness | Consumer / Health & Wellness |
| BAK | Chemicals / Specialty Materials | Specialty Chemicals / Specialty Materials |
| JOBY | Drones | Defense & Aerospace / Drones |
| BBNX | Individual Episodic Pivots / Singletons | MedTech / Diabetes |
| INDI | Automotive / Semiconductors | Automotive / ADAS | Automotive (Legacy) / Semiconductors | EV & Autonomous / ADAS & LiDAR |
| HSAI | LiDAR & AV Tech | EV & Autonomous / ADAS & LiDAR |
