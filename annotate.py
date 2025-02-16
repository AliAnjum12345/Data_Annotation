import aiohttp
import asyncio
import os
import pandas as pd
import google.generativeai as genai
import time
import random
import math
from bs4 import BeautifulSoup
import validators

# Set Gemini API key
NEW_GEMINI_API_KEY = "AIzaSyCIu5m1UXc0IhYYHrNE1wBsgfWEoeAf5ig"
genai.configure(api_key=NEW_GEMINI_API_KEY)

# File paths
input_csv_path = "C:\\Users\\Dell\\Downloads\\Data_Scrapping\\output.csv"
output_excel_path = "C:\\Users\\Dell\\Downloads\\Data_Scrapping\\annotation.xlsx"

# Define valid categories
CATEGORY_LIST = [
    "Deep Learning",
    "Reinforcement Learning",
    "Optimization",
    "Graph Neural Networks"
]

# Load the input CSV file into a DataFrame
data_df = pd.read_csv(input_csv_path)

# Determine already processed titles from the output Excel file (if it exists)
processed_titles = set()
if os.path.exists(output_excel_path):
    try:
        existing_df = pd.read_excel(output_excel_path)
        for title in existing_df["Title"]:
            processed_titles.add(title)
    except Exception as err:
        print(f"[Warning] Could not read existing Excel file: {err}")

async def retrieve_abstract(session, link_url):
    """
    Asynchronously fetch the abstract from a given paper link.
    Returns the text from the third <p> tag if available.
    """
    if not validators.url(link_url):
        return "Abstract not available."

    for _ in range(5):
        try:
            async with session.get(link_url, timeout=60) as response:
                if response.status == 200:
                    page_text = await response.text()
                    soup = BeautifulSoup(page_text, "html.parser")
                    paragraph_tags = soup.body.find_all("p")
                    if len(paragraph_tags) > 2:
                        return paragraph_tags[2].text.strip()
                    else:
                        return "Abstract not available."
        except Exception:
            pass
        await asyncio.sleep(2)
    return "Abstract not available."

async def fetch_abstract_with_sem(session, link_url, sem):
    async with sem:
        return await retrieve_abstract(session, link_url)

async def fetch_all_abstracts(url_list, max_simultaneous=5):
    """
    Fetch abstracts concurrently from a list of paper links.
    """
    semaphore = asyncio.Semaphore(max_simultaneous)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_abstract_with_sem(session, url, semaphore) for url in url_list]
        return await asyncio.gather(*tasks)

def classify_batch(paper_entries):
    """
    Classify a batch of papers using Google Gemini.
    Each paper is a tuple (title, abstract).
    """
    prompt_message = "You are an AI classifier. Categorize each paper into ONE of the following categories ONLY:\n"
    for idx, cat in enumerate(CATEGORY_LIST, start=1):
        prompt_message += f"{idx}. {cat}\n"
    
    prompt_message += "\nHere are the research papers:\n"
    for i, (title, abstract) in enumerate(paper_entries, start=1):
        prompt_message += f"\nPaper {i}:\nTitle: {title}\nAbstract: {abstract}\n"
    
    prompt_message += "\nRespond with a numbered list of categories for each paper. Only return category names from the list above."
    
    for attempt in range(5):
        try:
            model_instance = genai.GenerativeModel("gemini-pro")
            response = model_instance.generate_content(prompt_message)
            if not response.text:
                raise ValueError("Empty API response.")
            
            response_lines = response.text.strip().split("\n")
            classified_list = [
                next((cat for cat in CATEGORY_LIST if cat in line), "Unknown") 
                for line in response_lines
            ]
            while len(classified_list) < len(paper_entries):
                classified_list.append("Unknown")
            while len(classified_list) > len(paper_entries):
                classified_list.pop()
            return classified_list
        except Exception:
            time.sleep(5 * (2 ** attempt))
    return ["Unknown"] * len(paper_entries)

async def process_all_papers():
    """
    Process all research papers in batches:
    - Fetch abstracts
    - Classify them using Google Gemini
    - Save the results to an Excel file.
    """
    BATCH_SIZE = 10
    total_batches = math.ceil(len(data_df) / BATCH_SIZE)
    all_results = []  # List to store rows for Excel
    
    for batch_index in range(total_batches):
        start_index = batch_index * BATCH_SIZE
        end_index = min(start_index + BATCH_SIZE, len(data_df))
        batch_papers = []  # Holds (title, abstract) tuples for classification
        batch_links = []   # Holds paper links for fetching abstracts
        meta_info = []     # Holds meta data: (Year, Title, Authors, Paper Link)
        
        for idx in range(start_index, end_index):
            row = data_df.iloc[idx]
            paper_year = row["Year"]
            paper_title = row["Title"]
            paper_authors = row["Authors"]
            paper_url = row["Paper Link"]
            
            if paper_title in processed_titles:
                continue
            
            batch_links.append(paper_url)
            batch_papers.append((paper_title, None))  # Placeholder for abstract
            meta_info.append((paper_year, paper_title, paper_authors, paper_url))
        
        if not batch_papers:
            continue
        
        print(f"[Info] Batch {batch_index+1}/{total_batches}: Fetching abstracts...")
        abstracts_list = await fetch_all_abstracts(batch_links)
        batch_papers = [(title, abstract) for ((title, _), abstract) in zip(batch_papers, abstracts_list)]
        
        print("[Info] Abstracts fetched. Classifying papers...")
        assigned_categories = classify_batch(batch_papers)
        print("[Info] Classification complete. Saving results for this batch...")
        
        for i in range(len(batch_papers)):
            year_val, title_val, authors_val, link_val = meta_info[i]
            abstract_val = batch_papers[i][1]
            category_val = assigned_categories[i]
            all_results.append({
                "Year": year_val,
                "Title": title_val,
                "Authors": authors_val,
                "Paper Link": link_val,
                "Abstract": abstract_val,
                "Category": category_val
            })
            processed_titles.add(title_val)
        
        print(f"[Success] Batch {batch_index+1} processed.")
        time.sleep(random.uniform(40, 60))
    
    if os.path.exists(output_excel_path):
        try:
            existing_df = pd.read_excel(output_excel_path)
            combined_df = pd.concat([existing_df, pd.DataFrame(all_results)], ignore_index=True)
        except Exception as e:
            print(f"[Error] Issue reading existing Excel file: {e}")
            combined_df = pd.DataFrame(all_results)
    else:
        combined_df = pd.DataFrame(all_results)
    
    combined_df.to_excel(output_excel_path, index=False)
    print(f"[Done] All batches processed! Results saved to: {output_excel_path}")

# Run the asynchronous process
asyncio.run(process_all_papers())
