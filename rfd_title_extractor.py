import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd
import time
import html
from tqdm import tqdm

"""### **Get the 10-K filing based on company cik and filing year**
#### **Steps:**
1. Search for the correct filing using the cik and filing year in starting from fourth quarter to the first in SEC master index, break when found.
2. Extract actual filing date from the master index entry.
3. Extract the .txt path found in the master index and construct .htm path. Do the same for amended filing.
4. From the index url, parse the table content of the page to get the 10-K html path.
5. Read and return the 10-K content and the actual filing date.


"""

def fetch_from_url(url):
    """Downloads text content (like HTML or plain text) from a given URL."""

    HEADERS = {
        "User-Agent": "RibkaT-RiskExtractor/1.0 (contact: ribka.tiruneh@gwu.edu)"
    }
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()  # Raises HTTPError for bad status codes
        return response.text
    except requests.HTTPError as http_err:
        raise RuntimeError(f"Failed to fetch URL: {url} | HTTP Error: {http_err}")
    except Exception as e:
        raise RuntimeError(f"Error fetching URL {url}: {e}")

def find_10k_filing_info_from_master_index(content, cik):
    """Scans master.idx content to find the 10-K path and filing date."""

    cik_str = str(cik)
    lines = content.splitlines()
    _10K_info = []
    for idx, line in enumerate(lines):
        parts = line.split("|")
        if len(parts) == 5:
            cik_line, _, form_type, date_filed, path = parts
            if cik_line.strip() == cik_str and form_type.strip() == "10-K":
                _10K_info.append((path.strip(), date_filed.strip()))

                # Check the line above for amended filing:
                line = lines[idx - 1]
                parts = line.split("|")
                if len(parts) == 5:
                    cik_line, _, form_type, date_filed, path = parts
                    if cik_line.strip() == cik_str and form_type.strip() == "10-K/A":
                        _10K_info.append((path.strip(), date_filed.strip()))
                return _10K_info

    return _10K_info

def get_10K_document_url(sec_base_url, cik, filing_path):
    """Get the URL for the 10-K document from the table."""

    accession_base = filing_path.replace(".txt", "").strip()
    index_url = f"{sec_base_url}/Archives/{accession_base}-index.htm"
    print(f"Fetching SEC document index page: {index_url}")

    html_index = fetch_from_url(index_url)
    soup = BeautifulSoup(html_index, "html.parser")
    table = soup.find("table", class_="tableFile")
    if not table:
       raise RuntimeError(f"No document table found in index HTML for CIK {cik}")

    # Look for row with 10-K description and .htm file
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) >= 3:
            description = cols[1].get_text(strip=True)
            file_name = cols[2].get_text(strip=True)
            file_type = cols[3].get_text(strip=True)
            if ".htm" in file_name and ("10-K" in description or "10-K" in file_type):
                href = cols[2].a["href"]
                return f"{sec_base_url}{href}"

    raise RuntimeError(f"No 10-K document url found for CIK {cik}")

from os import path
def read_10K_filling(cik, filing_year, filing_order):
    """ Reads the 10-K content"""

    sec_base_url = "https://www.sec.gov"
    _10K_submission_path = None
    path_segment = None
    date_filed = None

    # Loop over the four quarters, tried using only the fourth quarter and failed as most companies file in either the first or third quarter.
    for quarter in [1, 3, 2, 4]:

        master_index_url = f"{sec_base_url}/Archives/edgar/full-index/{filing_year}/QTR{quarter}/master.idx"
        print(f"Checking master index: {master_index_url}")
        master_idx_content = fetch_from_url(master_index_url)

        if not master_idx_content:
            continue

        _10K_info = find_10k_filing_info_from_master_index(master_idx_content, cik)
        if filing_order == "Original" and _10K_info:
            path_segment, date_filed = _10K_info[0]
        elif filing_order == "Amended" and len(_10K_info)==2:
            path_segment, date_filed = _10K_info[1]
        elif filing_order == "Amended" and _10K_info: # return the original if there is no amended filing
            path_segment, date_filed = _10K_info[0]


        if path_segment and date_filed:
            print(f"Found 10-K in quarter {quarter}")
            _10K_submission_path = path_segment
            dt = datetime.strptime(date_filed, "%Y-%m-%d")
            actual_filing_date =  f"{dt.month}/{dt.day}/{dt.year}"
            print(f"Found 10-K for cik:{cik}: {_10K_submission_path} on {actual_filing_date}")
            _10K_url = get_10K_document_url(sec_base_url, cik, _10K_submission_path)
            print(f"10-K URL: {_10K_url}")
            _10K_html_content = fetch_from_url(_10K_url)

            return _10K_html_content, actual_filing_date


    raise ValueError(f"Could not read 10-K filing for CIK {cik} in {filing_year}")

"""### **Extraction**

#### 1. **Reporting Date**
- Look for date reporting patterns in the 10-K HTML content.

#### 2. **Item 1A Extraction**
- Regex pattern matching while avoiding table of content.

#### 3. **RFT Extraction**
- Filter based on length.
- Check for both HTML tags and style attributes.


"""

def extract_reporting_date(html_content):
    """ Extracts the reporting date from 10-K HTML content """

    patterns = [
        r"for\s+the\s+fiscal\s+year\s+ended\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})",
        r"fiscal\s+year\s+(?:ended|ending)\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})",
        r"for\s+the\s+year\s+(?:ended|ending)\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})",
        r"year\s+ended\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})",
        r"period\s+of\s+report\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})",
        r"conformed\s+period\s+of\s+report[:\s]*([0-9]{8})",
        r"for\s+the\s+fiscal\s+year\s+ended\s*([0-9]{8})",
        r"for\s+the\s+year\s+ended\s*([0-9]{8})"
    ]
    soup = BeautifulSoup(html_content, 'lxml')
    text = soup.get_text(separator=' ', strip=True)

    text = re.sub(r'\s+', ' ', text)
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1).strip()
            # YYYYMMDD format
            if len(date_str) == 8 and date_str.isdigit():
                try:
                   dt = datetime.strptime(date_str, "%Y%m%d")
                   reporting_date=  f"{dt.month}/{dt.day}/{dt.year}"
                   return reporting_date
                except ValueError:
                    continue
            # Month, DD, YYYY format
            for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
                try:
                   dt = datetime.strptime(date_str, fmt)
                   reporting_date=  f"{dt.month}/{dt.day}/{dt.year}"
                   return reporting_date
                except ValueError:
                    continue
    raise ValueError("Reporting date not found.")

def get_item_1a(html_content):
    """Extracts the full HTML for 'Item 1A. Risk Factors' """

    # Heuristics
    NEXT_CONTENT_LEN = 10 # the minimum number of words in the following paragraph
    HEADER_LEN = 5  # to avoid match embedded between text (in number of words)

    html_content = html.unescape(html_content)
    soup = BeautifulSoup(html_content, 'lxml')
    elements = soup.find_all(True, recursive=True)

    item_1a_pattern =re.compile(r"item(?:\s|&nbsp;|<[^>]+>)*1a\.?", re.IGNORECASE)
    item_1b_pattern = re.compile(r"item(?:\s|&nbsp;|<[^>]+>)*1b\.?", re.IGNORECASE)

    risk_factors_pattern = re.compile(r"Risk\s*Factors", re.IGNORECASE)

    start_idx = None
    found_start = False

    # Locate the first "Item 1A" heading that is not in a TOC and not embedded inside a paragraph.
    for i, tag in enumerate(elements):
      text = tag.get_text(" ", strip=True)
      if item_1a_pattern.fullmatch(text) or (item_1a_pattern.search(text) and len(text.split()) <= HEADER_LEN):
          has_content = False
          lookahead = elements[i:i+10]
          has_content = False
          for t in lookahead:
              t_text = t.get_text(" ", strip=True)
              if not t_text:
                  continue
              if len(t_text.split()) > NEXT_CONTENT_LEN:
                  has_content = True
                  break
          if has_content:
              start_idx = i
              found_start = True
              break

    if not found_start:
        print("Could not find 'Item 1A' start")
        return ""

    # Collect all tags from start_idx up to item 1b
    section_tags = []
    for idx,  tag in enumerate(elements[start_idx:]):
        text = tag.get_text(" ", strip=True)
        if item_1b_pattern.search(text) and len(text.split()) < NEXT_CONTENT_LEN :
            break
        section_tags.append(tag)

    def _slice_between_1a_1b(section_tags):
        start_idx = None
        end_idx = None
        for idx, tag in enumerate(section_tags):
            text = tag.get_text(" ", strip=True)
            if start_idx is None and item_1a_pattern.search(text):
                start_idx = idx
            if start_idx is not None and item_1b_pattern.search(text):
                end_idx = idx
                break
        return section_tags[start_idx:end_idx+1]

    def _smallest_common_parent(tags):
        ancestors = [t for t in tags[0].parents]
        for anc in ancestors:
            if all(anc in [t for t in tag.parents] for tag in tags):
                return anc
        return tags[0].parent  # fallback

    if not section_tags:
        print("Could not find 'Item 1A' end.")
        return ""

    # section_tags = _slice_between_1a_1b(section_tags)
    section_parent = _smallest_common_parent(section_tags)
    collected_blocks = []
    seen_blocks = set()
    for child in section_parent.find_all(recursive=True):
        if child not in section_tags:
            continue
        text = child.get_text(" ", strip=True)
        norm_text = " ".join(text.split()).lower()
        if not norm_text:
            continue
        if norm_text not in seen_blocks:
            seen_blocks.add(norm_text)
            collected_blocks.append(str(child))
    return "<div>\n" + "\n".join(collected_blocks) + "\n</div>"

def extract_risk_factor_titles(item_1a_html):
    """Extract a list of risk factor titles from the 'Item 1A. Risk Factors' section of a 10-K filing HTML."""

    # Heuristics
    MINIMUM_TITLE_LENGTH = 10
    MINIMUM_TITLE_LENGTH_PHRASE = 5
    MAXIMUM_TITLE_LENGTH = 1000

    soup = BeautifulSoup(item_1a_html, 'lxml')
    seen = set()  # to avoid duplicates
    titles = []    # to keep the order
    bold_tags = ["b", "strong", "h4"]
    italic_tags = ["i", "em"]
    underline_tags = ["u"]


    for tag in soup.find_all(True, recursive=True):
        text = tag.get_text(" ", strip=True)
        text = re.sub(r'\s+', ' ', text).strip()

        if not text or len(text) > MAXIMUM_TITLE_LENGTH or len(text.split()) < MINIMUM_TITLE_LENGTH_PHRASE or len(text) < MINIMUM_TITLE_LENGTH or text[-1] not in [":","."]:
            continue
      #
        # HTML formats
        if tag.name in bold_tags or tag.name in italic_tags or tag.name in underline_tags:
          if text not in seen and  not text.isupper():
              seen.add(text)
              titles.append({"text":text, "emphasis": tag.name})

        # CSS formats
        elif tag.name in ['p', 'div', 'span']:
          # Handle when the style is in the parent itself
          style = tag.get('style', '').lower().replace(" ", "")
          is_bold = 'font-weight:bold' in style or 'font-weight: bold' in style or 'font-weight:700' in style
          is_italic = 'font-style:italic' in style or 'font-style: italic' in style or 'font-style:700' in style
          is_underline = 'text-decoration:underline' in style or 'text-decoration: underline' in style

          if is_bold and text not in seen and not text.isupper():
              seen.add(text)
              titles.append({"text":text, "emphasis": "b"})
          elif is_italic and text not in seen and not text.isupper():
              seen.add(text)
              titles.append({"text":text, "emphasis": "i"})
          elif is_underline and text not in seen and not text.isupper():
              seen.add(text)
              titles.append({"text":text, "emphasis": "u"})

          # The following if block is added only to handle bad formating case where the period is split from the text
          # <p style="font: 10pt Times New Roman, Times, Serif; margin: 0pt 0; text-align: justify"><i>The conditional conversion feature
          # of the Notes, if triggered, may adversely affect our financial condition and operating results</i>.</p>
          child = tag.find("i", recursive=False)
          if child:
              text = child.get_text(" ", strip=True).strip()
              text = re.sub(r'\s+', ' ', text).strip()
              text = text.strip().rstrip('.').strip()
              text+= "."
              if len(text) <= MAXIMUM_TITLE_LENGTH and len(text) >= MINIMUM_TITLE_LENGTH_PHRASE and len(text) >= MINIMUM_TITLE_LENGTH:
                  if text not in seen and not text.isupper():
                    seen.add(text)
                    titles.append({"text":text, "emphasis": "i"})


          # Handle when the style is in the child
          final_text_bold, final_text_underline, final_text_italic = "","",""
          for child in tag.find_all(True, recursive=True):
              style = child.get('style', '').lower().replace(" ", "")
              is_bold = 'font-weight:bold' in style or 'font-weight: bold' in style or 'font-weight:700' in style
              is_italic = 'font-style:italic' in style or 'font-style: italic' in style or 'font-style:700' in style
              is_underline = 'text-decoration:underline' in style or 'text-decoration: underline' in style

              if is_bold:
                 text = child.get_text(" ", strip=True).strip()
                 text = re.sub(r'\s+', ' ', text).strip()
                 if text:
                    final_text_bold += text
              elif is_italic:
                 text = child.get_text(" ", strip=True).strip()
                 text = re.sub(r'\s+', ' ', text).strip()
                 if text:
                    final_text_italic += text
              elif is_underline:
                 text = child.get_text(" ", strip=True).strip()
                 text = re.sub(r'\s+', ' ', text).strip()
                 if text:
                    final_text_underline += text


          if final_text_bold and  len(final_text_bold) <= MAXIMUM_TITLE_LENGTH and len(final_text_bold.split()) >= MINIMUM_TITLE_LENGTH_PHRASE and len(final_text_bold) >= MINIMUM_TITLE_LENGTH and final_text_bold[-1] in [":","."]:
             if final_text_bold and final_text_bold not in seen and not final_text_bold.isupper():
                  seen.add(final_text_bold.strip())
                  titles.append({"text":final_text_bold, "emphasis": "b"})


          elif final_text_italic and len(final_text_italic) <= MAXIMUM_TITLE_LENGTH and len(final_text_italic.split()) >= MINIMUM_TITLE_LENGTH_PHRASE and len(final_text_italic) >= MINIMUM_TITLE_LENGTH and final_text_italic[-1] in [":","."]:
              if final_text_italic and final_text_italic not in seen and not final_text_italic.isupper():
                  seen.add(final_text_italic.strip())
                  titles.append({"text":final_text_italic, "emphasis": "i"})

          elif final_text_underline and len(final_text_underline) <= MAXIMUM_TITLE_LENGTH and len(final_text_underline.split()) >= MINIMUM_TITLE_LENGTH_PHRASE and len(final_text_underline) >= MINIMUM_TITLE_LENGTH and final_text_underline[-1] in [":","."]:
              if final_text_underline and final_text_underline not in seen and not final_text_underline.isupper():
                  seen.add(final_text_underline.strip())
                  titles.append({"text":final_text_underline, "emphasis": "u"})



    #  Handle the case where there are both italics and bold texts in item 1A section
    count_italic = sum(1 for t in titles if t["emphasis"] in italic_tags)
    count_bold = sum(1 for t in titles if t["emphasis"] in bold_tags)
    count_underline = sum(1 for t in titles if t["emphasis"] in underline_tags)

    if count_italic > count_bold and count_italic > count_underline:
        titles = [t for t in titles if t["emphasis"] in italic_tags]
    elif count_bold > count_italic and count_bold > count_underline:
        titles = [t for t in titles if t["emphasis"] in bold_tags]
    elif count_underline > count_italic and count_underline > count_bold:
        titles = [t for t in titles if t["emphasis"] in underline_tags]


    return [t["text"]for t in titles]

def main(input_csv_path):
    """Main pipeline function."""

    df_input = pd.read_csv(input_csv_path, dtype={'cik': str,
                                                  'filingyear': str,
                                                  'filingdate': str,
                                                  'reportingdate': str,
                                                  'RFDTitle': str
                                                 })
    results = []
    start_time = time.time()

    for _, row in tqdm(df_input.iterrows(), total=len(df_input), desc="Processing Filings"):

        cik = row['cik'].strip()
        filing_year = row['filingyear'].strip()
        print(f"____________________Processing cik: {cik} for filing year: {filing_year}_______________________")
        _10K_html_content, filing_date = read_10K_filling(cik, filing_year, "Original")
        item_1a_html = get_item_1a(_10K_html_content)
        # print(f"Item 1A HTML: {item_1a_html}")
        if not item_1a_html:
           _10K_html_content, filing_date = read_10K_filling(cik, filing_year, "Amended")
           item_1a_html = get_item_1a(_10K_html_content)

        reporting_date = extract_reporting_date(_10K_html_content)
        titles = extract_risk_factor_titles(item_1a_html)
        print(f"The number of RF titles: {len(titles)}")
        print(f"Risk Factor Titles: {titles}")
        if titles:
          for title in titles:
              results.append({
                  "cik": cik,
                  "filingyear": filing_year,
                  "filingdate": filing_date,
                  "reportingdate": reporting_date,
                  "RFDTitle": title.strip()
              })
        else:
          results.append({
                  "cik": cik,
                  "filingyear": filing_year,
                  "filingdate": filing_date,
                  "reportingdate": reporting_date,
                  "RFDTitle": ""
              })
    df_output = pd.DataFrame(results, columns=df_input.columns.tolist())
    print(f"Completed in {round((time.time() - start_time)/60, 2)} minutes.")
    df_output.to_csv("my_rasamplemini_rfdtitle_output.csv", index=False)
    # print(df_output.head())
    return df_output

if __name__ == "__main__":
    main("rasamplemini_rfdtitle.csv")