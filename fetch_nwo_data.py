#!/usr/bin/env python3
"""
Fetch NWO project data and extract collaboration information.
Queries ROR API for institution coordinates and outputs JSON for visualization.
"""

import json
import time
import requests
from collections import defaultdict
from itertools import combinations

# API endpoints
NWO_API = "https://nwopen-api.nwo.nl/NWOpen-API/api/Projects"
ROR_API = "https://api.ror.org/organizations"

# Dutch institution coordinates lookup (for when ROR ID is missing)
# Format: "organisation name pattern": {"name": display_name, "lat": lat, "lng": lng, "country": "The Netherlands", "country_code": "NL"}
DUTCH_INSTITUTIONS = {
    "Technische Universiteit Delft": {"name": "Delft University of Technology", "lat": 52.0024, "lng": 4.3736},
    "TU Delft": {"name": "Delft University of Technology", "lat": 52.0024, "lng": 4.3736},
    "Delft University of Technology": {"name": "Delft University of Technology", "lat": 52.0024, "lng": 4.3736},
    "Technische Universiteit Eindhoven": {"name": "Eindhoven University of Technology", "lat": 51.4478, "lng": 5.4908},
    "TU Eindhoven": {"name": "Eindhoven University of Technology", "lat": 51.4478, "lng": 5.4908},
    "Eindhoven University of Technology": {"name": "Eindhoven University of Technology", "lat": 51.4478, "lng": 5.4908},
    "Universiteit Twente": {"name": "University of Twente", "lat": 52.2389, "lng": 6.8497},
    "University of Twente": {"name": "University of Twente", "lat": 52.2389, "lng": 6.8497},
    "Rijksuniversiteit Groningen": {"name": "University of Groningen", "lat": 53.2194, "lng": 6.5665},
    "University of Groningen": {"name": "University of Groningen", "lat": 53.2194, "lng": 6.5665},
    "Universiteit Utrecht": {"name": "Utrecht University", "lat": 52.0853, "lng": 5.1214},
    "Utrecht University": {"name": "Utrecht University", "lat": 52.0853, "lng": 5.1214},
    "Universiteit Leiden": {"name": "Leiden University", "lat": 52.1575, "lng": 4.4854},
    "Leiden University": {"name": "Leiden University", "lat": 52.1575, "lng": 4.4854},
    "Universiteit van Amsterdam": {"name": "University of Amsterdam", "lat": 52.3556, "lng": 4.9550},
    "University of Amsterdam": {"name": "University of Amsterdam", "lat": 52.3556, "lng": 4.9550},
    "UvA": {"name": "University of Amsterdam", "lat": 52.3556, "lng": 4.9550},
    "Vrije Universiteit Amsterdam": {"name": "Vrije Universiteit Amsterdam", "lat": 52.3340, "lng": 4.8659},
    "VU Amsterdam": {"name": "Vrije Universiteit Amsterdam", "lat": 52.3340, "lng": 4.8659},
    "Erasmus Universiteit Rotterdam": {"name": "Erasmus University Rotterdam", "lat": 51.9173, "lng": 4.5260},
    "Erasmus University Rotterdam": {"name": "Erasmus University Rotterdam", "lat": 51.9173, "lng": 4.5260},
    "Radboud Universiteit": {"name": "Radboud University Nijmegen", "lat": 51.8205, "lng": 5.8659},
    "Radboud University": {"name": "Radboud University Nijmegen", "lat": 51.8205, "lng": 5.8659},
    "Radboud University Nijmegen": {"name": "Radboud University Nijmegen", "lat": 51.8205, "lng": 5.8659},
    "Maastricht University": {"name": "Maastricht University", "lat": 50.8465, "lng": 5.6872},
    "Universiteit Maastricht": {"name": "Maastricht University", "lat": 50.8465, "lng": 5.6872},
    "Tilburg University": {"name": "Tilburg University", "lat": 51.5648, "lng": 5.0434},
    "Universiteit van Tilburg": {"name": "Tilburg University", "lat": 51.5648, "lng": 5.0434},
    "Wageningen University": {"name": "Wageningen University & Research", "lat": 51.9692, "lng": 5.6654},
    "Wageningen University & Research": {"name": "Wageningen University & Research", "lat": 51.9692, "lng": 5.6654},
    "WUR": {"name": "Wageningen University & Research", "lat": 51.9692, "lng": 5.6654},
    "Open Universiteit": {"name": "Open University of the Netherlands", "lat": 50.8882, "lng": 5.9808},
    "Open University": {"name": "Open University of the Netherlands", "lat": 50.8882, "lng": 5.9808},
    # Medical centers
    "Erasmus MC": {"name": "Erasmus MC", "lat": 51.9225, "lng": 4.4792},
    "LUMC": {"name": "Leiden University Medical Center", "lat": 52.1667, "lng": 4.4792},
    "Leiden University Medical Center": {"name": "Leiden University Medical Center", "lat": 52.1667, "lng": 4.4792},
    "UMC Utrecht": {"name": "University Medical Center Utrecht", "lat": 52.0875, "lng": 5.1786},
    "University Medical Center Utrecht": {"name": "University Medical Center Utrecht", "lat": 52.0875, "lng": 5.1786},
    "UMCG": {"name": "University Medical Center Groningen", "lat": 53.2217, "lng": 6.5756},
    "University Medical Center Groningen": {"name": "University Medical Center Groningen", "lat": 53.2217, "lng": 6.5756},
    "Radboudumc": {"name": "Radboud University Medical Center", "lat": 51.8425, "lng": 5.8528},
    "Radboud University Medical Center": {"name": "Radboud University Medical Center", "lat": 51.8425, "lng": 5.8528},
    "Amsterdam UMC": {"name": "Amsterdam University Medical Centers", "lat": 52.3340, "lng": 4.8659},
    "Amsterdam University Medical Centers": {"name": "Amsterdam University Medical Centers", "lat": 52.3340, "lng": 4.8659},
    "VUmc": {"name": "Amsterdam UMC Location Vrije Universiteit Amsterdam", "lat": 52.3340, "lng": 4.8617},
    "Maastricht UMC": {"name": "Maastricht University Medical Centre", "lat": 50.8442, "lng": 5.6997},
    "Maastricht University Medical Centre": {"name": "Maastricht University Medical Centre", "lat": 50.8442, "lng": 5.6997},
    # Research institutes
    "KNAW": {"name": "Royal Netherlands Academy of Arts and Sciences", "lat": 52.3702, "lng": 4.8952},
    "Koninklijke Nederlandse Akademie van Wetenschappen": {"name": "Royal Netherlands Academy of Arts and Sciences", "lat": 52.3702, "lng": 4.8952},
    "Royal Netherlands Academy of Arts and Sciences": {"name": "Royal Netherlands Academy of Arts and Sciences", "lat": 52.3702, "lng": 4.8952},
    "NWO": {"name": "Netherlands Organisation for Scientific Research", "lat": 52.0840, "lng": 5.1261},
    "SURF": {"name": "SURF", "lat": 52.0894, "lng": 5.1086},
    "SURF - Coöperatie SURF U.A.": {"name": "SURF", "lat": 52.0894, "lng": 5.1086},
    "Netherlands eScience Center": {"name": "Netherlands eScience Center", "lat": 52.3550, "lng": 4.9547},
    "DANS": {"name": "Data Archiving and Networked Services", "lat": 52.0840, "lng": 5.1261},
    "KB": {"name": "National Library of the Netherlands", "lat": 52.0799, "lng": 4.3276},
    "Koninklijke Bibliotheek": {"name": "National Library of the Netherlands", "lat": 52.0799, "lng": 4.3276},
    "National Library of the Netherlands": {"name": "National Library of the Netherlands", "lat": 52.0799, "lng": 4.3276},
    # Universities of Applied Sciences
    "Hogeschool Utrecht": {"name": "University of Applied Sciences Utrecht", "lat": 52.0840, "lng": 5.1750},
    "HU": {"name": "University of Applied Sciences Utrecht", "lat": 52.0840, "lng": 5.1750},
    "Hogeschool van Amsterdam": {"name": "Amsterdam University of Applied Sciences", "lat": 52.3590, "lng": 4.9088},
    "HvA": {"name": "Amsterdam University of Applied Sciences", "lat": 52.3590, "lng": 4.9088},
    "Hogeschool Rotterdam": {"name": "Rotterdam University of Applied Sciences", "lat": 51.9170, "lng": 4.4846},
    "Fontys": {"name": "Fontys University of Applied Sciences", "lat": 51.4512, "lng": 5.4823},
    "Fontys Hogescholen": {"name": "Fontys University of Applied Sciences", "lat": 51.4512, "lng": 5.4823},
    "Saxion": {"name": "Saxion University of Applied Sciences", "lat": 52.2215, "lng": 6.8937},
    "Hanzehogeschool": {"name": "Hanze University of Applied Sciences", "lat": 53.2119, "lng": 6.5827},
    "Hanze University of Applied Sciences": {"name": "Hanze University of Applied Sciences", "lat": 53.2119, "lng": 6.5827},
}

def normalize_org_name(org_name):
    """Normalize organisation name for lookup."""
    if not org_name:
        return None
    # Get base name (before ||)
    base = org_name.split("||")[0].strip()
    return base

def fetch_nwo_projects():
    """Fetch all Open Science NL projects (project_id starting with 500.)."""
    all_projects = []
    page = 1

    print("Fetching Open Science NL projects (500.*)...")

    while True:
        try:
            params = {"project_id": "500.", "page": page, "pageSize": 100}
            response = requests.get(NWO_API, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            projects = data.get("projects", [])

            if not projects:
                break

            # Only include Open Science NL projects
            os_projects = [p for p in projects if p.get("funding_scheme", "").startswith("Open Science NL")]
            all_projects.extend(os_projects)
            print(f"  Page {page}: {len(os_projects)}/{len(projects)} Open Science NL projects (total: {len(all_projects)})")

            if len(projects) < 100:
                break
            page += 1
            time.sleep(0.3)
        except requests.exceptions.RequestException as e:
            print(f"  Error: {e}")
            break

    return all_projects

def extract_collaborations(projects):
    """Extract institution collaborations from projects."""
    print("Extracting collaborations...")

    institution_projects = defaultdict(list)  # key (ror_id or org_name) -> list of {id, title}
    # collaboration_pairs now stores: {(key1, key2): {scheme: count, ...}}
    collaboration_pairs = defaultdict(lambda: defaultdict(int))
    funding_schemes = set()
    all_projects_list = []  # All projects regardless of ROR
    projects_with_grant_id = 0
    org_name_institutions = {}  # org_name -> institution info (for fallback)

    for project in projects:
        grant_id = project.get("grant_id", "")  # DOI link
        project_id = project.get("project_id", "")  # Fallback identifier
        project_title = project.get("title", "Untitled")

        # Use grant_id (DOI) if available, otherwise use project_id
        identifier = grant_id.strip() if grant_id and grant_id.strip() else project_id.strip()

        # Skip projects without any valid identifier
        if not identifier:
            continue

        projects_with_grant_id += 1
        funding_scheme = project.get("funding_scheme", "Unknown")
        funding_schemes.add(funding_scheme)
        project_info = {"grant_id": identifier, "title": project_title[:60], "funding_scheme": funding_scheme}

        # Add to all projects list
        all_projects_list.append(project_info)

        # Get project members
        members = project.get("project_members", project.get("projectMembers", []))

        # Extract unique institution keys from this project (ROR ID or org name)
        institution_keys = set()
        if members:
            for member in members:
                ror = member.get("ror") or member.get("rorId") or member.get("institution_ror")
                org_name = member.get("organisation", "")

                # Prefer ROR ID if available
                if ror and ror != "-" and "ror.org/" in ror:
                    ror_id = ror.split("ror.org/")[-1]
                    institution_keys.add(("ror", ror_id))
                else:
                    # Fallback to organisation name lookup
                    base_org = normalize_org_name(org_name)
                    if base_org and base_org in DUTCH_INSTITUTIONS:
                        inst_info = DUTCH_INSTITUTIONS[base_org]
                        # Use normalized name as key
                        key = ("org", inst_info["name"])
                        institution_keys.add(key)
                        org_name_institutions[inst_info["name"]] = inst_info

        # Store project info for each institution
        for key in institution_keys:
            key_str = f"{key[0]}:{key[1]}"
            if not any(p["grant_id"] == identifier for p in institution_projects[key_str]):
                institution_projects[key_str].append(project_info)

        # Count collaboration pairs by funding scheme (projects with 2+ institutions)
        if len(institution_keys) >= 2:
            sorted_keys = sorted([f"{k[0]}:{k[1]}" for k in institution_keys])
            for pair in combinations(sorted_keys, 2):
                collaboration_pairs[pair][funding_scheme] += 1

    print(f"  Found {projects_with_grant_id} projects with grant IDs")
    print(f"  Found {len(institution_projects)} unique institutions")
    print(f"  Found {len(collaboration_pairs)} collaboration pairs")
    print(f"  Found {len(funding_schemes)} funding schemes")

    return institution_projects, collaboration_pairs, sorted(funding_schemes), all_projects_list, org_name_institutions

def fetch_ror_data(ror_ids):
    """Fetch institution details from ROR API."""
    print(f"Fetching data for {len(ror_ids)} institutions from ROR API...")

    institutions = {}

    for i, ror_id in enumerate(ror_ids):
        try:
            url = f"{ROR_API}/{ror_id}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # Extract location from geonames_details
                lat, lng = None, None
                country = "Unknown"
                country_code = ""

                locations = data.get("locations", [])
                if locations:
                    geo = locations[0].get("geonames_details", {})
                    lat = geo.get("lat")
                    lng = geo.get("lng")
                    country = geo.get("country_name", "Unknown")
                    country_code = geo.get("country_code", "")

                # Extract name from names array (ROR API v2)
                # Look for the 'ror_display' type first, then any label
                name = "Unknown"
                names = data.get("names", [])
                for n in names:
                    if "ror_display" in n.get("types", []):
                        name = n.get("value", "Unknown")
                        break
                if name == "Unknown":
                    for n in names:
                        if "label" in n.get("types", []):
                            name = n.get("value", "Unknown")
                            break

                institutions[ror_id] = {
                    "name": name,
                    "lat": lat,
                    "lng": lng,
                    "country": country,
                    "country_code": country_code,
                    "ror_id": ror_id
                }

                if (i + 1) % 10 == 0:
                    print(f"  Processed {i + 1}/{len(ror_ids)} institutions...")
            else:
                print(f"  Warning: Could not fetch ROR {ror_id} (status {response.status_code})")

            time.sleep(0.2)  # Rate limiting

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching ROR {ror_id}: {e}")

    return institutions

def main():
    # Fetch NWO projects
    projects = fetch_nwo_projects()

    if not projects:
        print("No projects fetched. Please check the API connection.")
        return

    print(f"Total projects fetched: {len(projects)}")

    # Extract collaborations
    institution_projects, collaboration_pairs, funding_schemes, all_projects_list, org_name_institutions = extract_collaborations(projects)

    if not institution_projects:
        print("No institutions found in projects.")
        return

    # Separate ROR-based and org-name-based institution keys
    ror_keys = [k for k in institution_projects.keys() if k.startswith("ror:")]
    org_keys = [k for k in institution_projects.keys() if k.startswith("org:")]

    # Fetch ROR data for ROR-based institutions
    ror_ids = [k.split(":", 1)[1] for k in ror_keys]
    ror_institutions = fetch_ror_data(ror_ids)

    # Build combined institutions dict (key -> institution info)
    all_institutions = {}

    # Add ROR-based institutions
    for key in ror_keys:
        ror_id = key.split(":", 1)[1]
        if ror_id in ror_institutions:
            all_institutions[key] = ror_institutions[ror_id]

    # Add org-name-based institutions from lookup table
    for key in org_keys:
        org_name = key.split(":", 1)[1]
        if org_name in org_name_institutions:
            inst = org_name_institutions[org_name]
            all_institutions[key] = {
                "name": inst["name"],
                "lat": inst["lat"],
                "lng": inst["lng"],
                "country": "The Netherlands",
                "country_code": "NL",
                "ror_id": None
            }

    # Build output data
    output_institutions = []
    for key, proj_list in institution_projects.items():
        if key in all_institutions:
            inst = all_institutions[key]
            if inst["lat"] is not None and inst["lng"] is not None:
                output_institutions.append({
                    "name": inst["name"],
                    "lat": inst["lat"],
                    "lng": inst["lng"],
                    "count": len(proj_list),
                    "projects": proj_list,  # All projects for this institution
                    "country": inst["country"],
                    "country_code": inst["country_code"],
                    "ror_id": inst.get("ror_id")
                })

    # Build collaboration links (top connections)
    output_links = []
    # Sort by total count across all schemes
    sorted_pairs = sorted(collaboration_pairs.items(), key=lambda x: -sum(x[1].values()))[:50]
    for (key1, key2), scheme_counts in sorted_pairs:
        if key1 in all_institutions and key2 in all_institutions:
            inst1, inst2 = all_institutions[key1], all_institutions[key2]
            if all([inst1["lat"], inst1["lng"], inst2["lat"], inst2["lng"]]):
                output_links.append({
                    "source": {"lat": inst1["lat"], "lng": inst1["lng"], "name": inst1["name"]},
                    "target": {"lat": inst2["lat"], "lng": inst2["lng"], "name": inst2["name"]},
                    "count": sum(scheme_counts.values()),  # Total count
                    "scheme_counts": dict(scheme_counts)   # Count per funding scheme
                })

    # Write output
    output = {
        "institutions": output_institutions,
        "links": output_links,
        "funding_schemes": funding_schemes,
        "all_projects": all_projects_list,
        "metadata": {
            "total_projects": len(all_projects_list),
            "total_institutions": len(output_institutions),
            "total_links": len(output_links),
            "total_funding_schemes": len(funding_schemes)
        }
    }

    with open("collaboration_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nData saved to collaboration_data.json")
    print(f"  - {len(output_institutions)} institutions with coordinates")
    print(f"  - {len(output_links)} collaboration links")

if __name__ == "__main__":
    main()
