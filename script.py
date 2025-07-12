import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from tqdm import tqdm
import time
import re
import os
import json
import uuid
import shutil
import msvcrt
from pathlib import Path
import traceback
import sys

# Helper function to click on provider elements
async def click_on_provider(page, provider_name):
    """
    Attempts to find and click on an element containing the provider name
    Returns True if successful, False otherwise
    """
    # 1. Try clicking on elements with the exact name
    elements_with_name = await page.query_selector_all(f'text="{provider_name}"')
    
    if elements_with_name and len(elements_with_name) > 0:
        for elem in elements_with_name:
            try:
                # Check if this element or its parent is clickable
                await elem.click()
                print(f"Found and clicked provider: {provider_name}")
                await page.wait_for_load_state("networkidle")
                return True
            except Exception:
                continue
    
    # 2. Try partial match or container elements
    item_selectors = [
        'tr[role="row"]', 
        '.directory-item',
        '.card',
        'table tbody tr',
        '[class*="institute"]',
        '[class*="item"]',
        'tr',
        'td',
        'div[class*="card"]',
        'a'
    ]
    
    for selector in item_selectors:
        items = await page.query_selector_all(selector)
        for item in items:
            try:
                text = await item.inner_text()
                if provider_name.lower() in text.lower():
                    await item.click()
                    print(f"Found and clicked container for: {provider_name}")
                    await page.wait_for_load_state("networkidle")
                    return True
            except Exception:
                continue
    
    # 3. Try clicking any links or buttons that might lead to details page
    detail_selectors = ['a[href*="detail"], button:has-text("Details"), a:has-text("Details"), [class*="detail"]']
    for selector in detail_selectors:
        try:
            detail_elems = await page.query_selector_all(selector)
            for elem in detail_elems:
                await elem.click()
                await page.wait_for_load_state("networkidle")
                # Check if we're on a details page
                page_text = await page.text_content('body')
                if provider_name.lower() in page_text.lower():
                    print(f"Found provider details page via details link: {provider_name}")
                    return True
                # If not on the right page, go back
                await page.goto(page.url)
                await page.wait_for_load_state("networkidle")
        except Exception:
            continue
    
    return False

async def click_and_extract_subprograms(page, element, main_title):
    """
    Clicks on a program header element and extracts any subprograms that appear.
    Returns a list of subprogram names.
    """
    print(f"Attempting to extract subprograms for: {main_title}")
    subprograms = []
    
    # Define a comprehensive list of navigation elements to filter out
    nav_elements = [
        'home', 'about', 'contact', 'menu', 'skip to content', 
        'services', 'about us', 'find', 'resources', 'guides', 
        'participate', 'search', 'login', 'register', 'sign in',
        'education institutions', 'find education', 'copyright',
        'privacy', 'terms', 'sitemap', 'faq', 'help',
        'click', 'toggle', 'close', 'open', 'show', 'hide', 
        'programs offered', 'next', 'previous', 'submit', 
        'apply now', 'learn more', 'read more', 'view all',
        'download', 'upload', 'back', 'forward', 'continue', 'proceed',
        'cancel', 'ok', 'yes', 'no', 'submit', 'reset', 'clear',
        'navigate', 'expand', 'collapse', 'menu', 'dropdown'
    ]
    
    # Function to check if text is likely a navigation element
    def is_nav_element(text):
        if not text or len(text.strip()) < 3:
            return True
        
        text_lower = text.lower().strip()
        
        # Check against our list of navigation elements
        if any(nav == text_lower or 
              (len(nav) > 4 and nav in text_lower) for nav in nav_elements):
            return True
            
        # Check for common navigation patterns
        if re.search(r'^\s*[\[\(\<\>\)\]]+\s*$', text):  # Just brackets/arrows
            return True
        if re.search(r'^\s*\d+\s*$', text):  # Just a number
            return True
        if re.search(r'^\s*[<>→←↑↓⇒⇐⇑⇓]\s*$', text):  # Just an arrow
            return True
        if len(text) < 5 and not re.search(r'[a-zA-Z]{2,}', text):  # Very short with no words
            return True
            
        # Not a navigation element
        return False
    
    # First, take a snapshot of the page content before clicking
    before_content = await page.evaluate("""
        () => {
            return Array.from(document.querySelectorAll('*'))
                .filter(el => el.offsetWidth > 0 && el.offsetHeight > 0 && el.innerText?.trim())
                .map(el => ({ text: el.innerText.trim(), visible: true }));
        }
    """)
    
    # Try clicking on the element
    try:
        # Standard click
        await element.click()
        await page.wait_for_timeout(1000)
    except Exception:
        # Try JavaScript click if standard fails
        try:
            await page.evaluate('(element) => element.click()', element)
            await page.wait_for_timeout(1000)
        except Exception as js_err:
            print(f"Both standard and JavaScript clicks failed: {js_err}")
    
    # Check if element has href or data-bs-target (typical for Bootstrap accordions)
    target_id = None
    try:
        href = await element.get_attribute('href')
        if href and href.startswith('#'):
            target_id = href[1:]
        else:
            data_target = await element.get_attribute('data-bs-target') or await element.get_attribute('data-target')
            if data_target and data_target.startswith('#'):
                target_id = data_target[1:]
    except Exception:
        pass
        
    # If we have a target ID, directly check that element
    if target_id:
        try:
            print(f"Found target container with ID: {target_id}")
            target_elem = await page.query_selector(f'#{target_id}')
            if target_elem:
                # Make sure it's visible
                await page.evaluate(f"""
                    (id) => {{
                        const elem = document.getElementById(id);
                        if (elem) {{
                            elem.classList.add('show');
                            elem.style.display = 'block';
                        }}
                    }}
                """, target_id)
                
                # Extract all text items within the target
                items = await page.evaluate("""
                    (id) => {
                        const container = document.getElementById(id);
                        if (!container) return [];
                        
                        // Extract all text nodes that could be subprograms
                        return Array.from(container.querySelectorAll('li, .list-item, p, div, span'))
                            .filter(el => el.offsetWidth > 0 && el.offsetHeight > 0) // Only visible items
                            .map(el => el.innerText.trim())
                            .filter(text => text.length > 0);
                    }
                """, target_id)
                
                if items and len(items) > 0:
                    print(f"Found {len(items)} items in target container")
                    for item in items:
                        if item and item not in subprograms:
                            subprograms.append(item)
        except Exception as e:
            print(f"Error processing target element: {e}")
    
    # If we didn't find subprograms via target ID, check for new elements after click
    if not subprograms:
        try:
            # Get the content after clicking, but look specifically for list items or structure
            after_content = await page.evaluate("""
                () => {
                    // Look for the most likely container of subprograms
                    let containers = [];
                    
                    // First look for lists that might have appeared
                    const lists = document.querySelectorAll('ul, ol');
                    lists.forEach(list => {
                        if (list.offsetWidth > 0 && list.offsetHeight > 0) { // Visible
                            const items = Array.from(list.querySelectorAll('li'))
                                .filter(li => li.offsetWidth > 0 && li.offsetHeight > 0)
                                .map(li => li.innerText.trim())
                                .filter(text => text.length > 0);
                                
                            if (items.length > 0) {
                                containers.push({
                                    type: 'list',
                                    items: items
                                });
                            }
                        }
                    });
                    
                    // If no lists, look for other containers that might have appeared
                    if (containers.length === 0) {
                        // Look for visible elements that might be containers
                        const possibleContainers = Array.from(document.querySelectorAll('.collapse.show, [aria-expanded="true"] + *, .card-body, .panel-body, .accordion-body'))
                            .filter(el => el.offsetWidth > 0 && el.offsetHeight > 0);
                            
                        possibleContainers.forEach(container => {
                            // Look for text items within this container
                            const items = Array.from(container.querySelectorAll('p, div, span, li'))
                                .filter(el => el.offsetWidth > 0 && el.offsetHeight > 0)
                                .map(el => el.innerText.trim())
                                .filter(text => text.length > 0);
                                
                            if (items.length > 0) {
                                containers.push({
                                    type: 'container',
                                    items: items
                                });
                            }
                        });
                    }
                    
                    return containers;
                }
            """)
            
            # Process the extracted containers
            if after_content and len(after_content) > 0:
                # Prefer lists over other containers
                list_containers = [c for c in after_content if c['type'] == 'list']
                if list_containers:
                    container = list_containers[0]  # Take the first list
                else:
                    container = after_content[0]  # Otherwise take the first container
                
                # Add items to subprograms
                print(f"Found {len(container['items'])} items in a {container['type']} after clicking")
                for item in container['items']:
                    # Skip items that are navigation elements or just the main title repeated
                    if item and item != main_title and item not in subprograms:
                        # Clean up the item text - keep only the first line if it's multi-line
                        clean_item = item.split('\n')[0].strip()
                        if clean_item and len(clean_item) > 2 and not is_nav_element(clean_item):
                            # Skip if very similar to main title (likely just a header repetition)
                            main_words = set(main_title.lower().split())
                            item_words = set(clean_item.lower().split())
                            # If item is just a subset of main title words or vice versa, skip it
                            if not (main_words.issubset(item_words) or item_words.issubset(main_words)):
                                subprograms.append(clean_item)
            else:
                # Fallback to checking for any new elements
                after_elements = await page.evaluate("""
                    () => {
                        return Array.from(document.querySelectorAll('*'))
                            .filter(el => el.offsetWidth > 0 && el.offsetHeight > 0 && el.innerText?.trim())
                            .map(el => ({ 
                                text: el.innerText.trim(),
                                isListItem: el.tagName === 'LI',
                                isParagraph: el.tagName === 'P' 
                            }));
                    }
                """)
                
                # Prioritize list items and paragraphs as they're more likely to be actual programs
                list_items = [item['text'] for item in after_elements if item['isListItem']]
                paragraphs = [item['text'] for item in after_elements if item['isParagraph']]
                
                if list_items:
                    print(f"Found {len(list_items)} list items after clicking")
                    for text in list_items:
                        if text != main_title and text not in subprograms:
                            clean_text = text.split('\n')[0].strip()
                            if clean_text and len(clean_text) > 2 and not is_nav_element(clean_text):
                                # Skip if very similar to main title
                                main_words = set(main_title.lower().split())
                                item_words = set(clean_text.lower().split())
                                if not (main_words.issubset(item_words) or item_words.issubset(main_words)):
                                    subprograms.append(clean_text)
                elif paragraphs:
                    print(f"Found {len(paragraphs)} paragraphs after clicking")
                    for text in paragraphs:
                        if text != main_title and text not in subprograms:
                            clean_text = text.split('\n')[0].strip()
                            if clean_text and len(clean_text) > 2 and not is_nav_element(clean_text):
                                # Skip if very similar to main title
                                main_words = set(main_title.lower().split())
                                item_words = set(clean_text.lower().split())
                                if not (main_words.issubset(item_words) or item_words.issubset(main_words)):
                                    subprograms.append(clean_text)
        except Exception as e:
            print(f"Error analyzing content after clicking: {e}")
    
    # If we still didn't find anything, look for list items or divs near the clicked element
    if not subprograms:
        try:
            # Try to find any lists after this element
            items = await page.evaluate("""
                (element) => {
                    const items = [];
                    let sibling = element.nextElementSibling;
                    
                    // Check up to 5 siblings after the clicked element
                    let count = 0;
                    while (sibling && count < 5) {
                        // If it's a list or contains list items, extract them
                        if (sibling.tagName === 'UL' || sibling.tagName === 'OL' || 
                            sibling.querySelector('li') || 
                            sibling.classList.contains('list')) {
                            
                            // Extract list items
                            const listItems = sibling.querySelectorAll('li');
                            if (listItems.length > 0) {
                                for (const li of listItems) {
                                    if (li.innerText.trim()) {
                                        items.push(li.innerText.trim());
                                    }
                                }
                            } else if (sibling.innerText.trim()) {
                                // If no list items found but sibling has text, use that
                                items.push(sibling.innerText.trim());
                            }
                        }
                        
                        sibling = sibling.nextElementSibling;
                        count++;
                    }
                    
                    return items;
                }
            """, element)
            
            if items:
                print(f"Found {len(items)} potential subprograms in nearby elements")
                for item in items:
                    if item != main_title and item not in subprograms:
                        clean_item = item.split('\n')[0].strip()
                        if clean_item and len(clean_item) > 2 and not is_nav_element(clean_item):
                            # Skip if very similar to main title
                            main_words = set(main_title.lower().split())
                            item_words = set(clean_item.lower().split())
                            if not (main_words.issubset(item_words) or item_words.issubset(main_words)):
                                subprograms.append(clean_item)
        except Exception as e:
            print(f"Error finding nearby list items: {e}")
    
    # Return the list of subprograms we found
    return subprograms

async def click_and_expand_accordion(page, text):
    """
    Finds and expands an accordion element containing the specified text.
    Returns the expanded content element if successful, None otherwise.
    """
    # Wait for page to be completely loaded
    await page.wait_for_load_state("networkidle")
    
    print(f"Looking for accordion with text: '{text}'")
    
    # Find accordion trigger with more robust selectors
    selectors = [
        f'a[role="button"][data-bs-toggle="collapse"]:has-text("{text}")',
        f'a[role="button"]:has-text("{text}")',
        f'button[data-bs-toggle="collapse"]:has-text("{text}")',
        f'[data-bs-toggle="collapse"]:has-text("{text}")',
        f'[data-toggle="collapse"]:has-text("{text}")',
        f'button.accordion-button:has-text("{text}")',
        f'.accordion-header:has-text("{text}")',
        f'.card-header:has-text("{text}")',
        f'.panel-heading:has-text("{text}")',
        f'a:has-text("{text}")',
        f'div.accordion:has-text("{text}")',
        f'div:has-text("{text}")',
        f'h3:has-text("{text}")',
        f'h4:has-text("{text}")',
        f'h5:has-text("{text}")',
        f'*:has-text("{text}")'
    ]
    
    # Try each selector
    element = None
    for selector in selectors:
        try:
            # Use a timeout to avoid hanging if element doesn't exist
            element = await page.wait_for_selector(selector, timeout=3000, state='attached')
            if element:
                print(f"Found accordion trigger with selector: {selector}")
                break
        except Exception:
            continue
    
    if not element:
        print(f"Could not find accordion trigger for '{text}'")
        return None
    
    try:
        # Check if the accordion is already expanded
        aria_expanded = await element.get_attribute('aria-expanded')
        
        # If not expanded, click to expand it
        if aria_expanded != 'true':
            print("Clicking accordion trigger")
            # First try the standard click method
            try:
                await element.click()
            except Exception:
                # Fallback to JavaScript click if standard click fails
                await page.evaluate('(element) => element.click()', element)
                
            # Wait for animation and for content to be visible
            await page.wait_for_timeout(1500)  # Longer wait time
        
        # Get target content ID from href or data-bs-target attribute
        target = await element.get_attribute('href') or await element.get_attribute('data-bs-target')
        print(f"Accordion target attribute: {target}")
        
        if target and target.startswith('#'):
            content_id = target[1:]
            try:
                print(f"Looking for accordion content with ID: {content_id}")
                # Wait for the content to be attached to the DOM
                content = await page.wait_for_selector(f'#{content_id}, div[id="{content_id}"]', 
                                                      state='attached', 
                                                      timeout=3000)
                if content:
                    # Check if content is visible
                    is_visible = await content.is_visible()
                    print(f"Accordion content visibility: {is_visible}")
                    
                    if not is_visible:
                        # Try to make it visible using JavaScript
                        print("Content not visible, trying to make it visible with JavaScript")
                        await page.evaluate(f"""
                            (id) => {{
                                const elem = document.getElementById(id);
                                if (elem) {{
                                    elem.classList.add('show');
                                    elem.style.display = 'block';
                                    elem.setAttribute('aria-expanded', 'true');
                                }}
                            }}
                        """, content_id)
                        await page.wait_for_timeout(500)
                    
                    return content
            except Exception as e:
                print(f"Error finding content with ID {content_id}: {e}")
        
        # If we couldn't find by ID, try using JavaScript to find the accordion content
        # This is more robust than using ElementHandles
        content_element = await page.evaluate("""
            (element) => {
                // Try to find the content by checking for expanded elements
                // First check if this is a standard Bootstrap accordion
                const target = element.getAttribute('href') || element.getAttribute('data-bs-target');
                if (target && target.startsWith('#')) {
                    const contentId = target.substring(1);
                    const content = document.getElementById(contentId);
                    if (content) return true; // We'll use the selector later
                }
                
                // Check if the next element is the content
                let nextElem = element.nextElementSibling;
                if (nextElem && 
                    (nextElem.classList.contains('collapse') || 
                     nextElem.classList.contains('content') ||
                     nextElem.classList.contains('panel'))) {
                    return true;
                }
                
                // Check if parent's next sibling contains content
                const parent = element.parentElement;
                if (parent) {
                    const nextSibling = parent.nextElementSibling;
                    if (nextSibling && 
                        (nextSibling.classList.contains('collapse') || 
                         nextSibling.classList.contains('content') ||
                         nextSibling.classList.contains('panel'))) {
                        return true;
                    }
                }
                
                return false;
            }
        """, element)
        
        if content_element:
            # If JavaScript found a content element, look for all possible content containers
            for content_selector in [
                '.collapse.show', 
                '.accordion-body',
                '.card-body',
                '.panel-body',
                '.content',
                '[role="tabpanel"]'
            ]:
                try:
                    content = await page.wait_for_selector(content_selector, timeout=2000)
                    if content:
                        return content
                except Exception:
                    continue
        
    except Exception as e:
        print(f"Error expanding accordion '{text}': {e}")
    
    # As a last resort, try to find any visible element that might contain the programs
    try:
        # Look for content that might be related to programs
        for content_selector in [
            'div:has-text("Programs")', 
            'div:has-text("Courses")',
            '[class*="program"]',
            '[class*="course"]'
        ]:
            content = await page.query_selector(content_selector)
            if content:
                return content
    except Exception:
        pass
    
    return None

async def scrape_training_providers():
    """
    Scrapes training providers data from KHDA website
    """
    # Create results directory if it doesn't exist
    import os  # Local import to ensure os is available in this scope
    import json  # Also import json which is needed in this function
    import pandas as pd  # Import pandas for DataFrame operations
    import re  # Import re for regular expressions
    from playwright.async_api import async_playwright
    import asyncio  # Import asyncio for sleep
    
    os.makedirs('results', exist_ok=True)
    
    # Initialize variables at the start
    detailed_providers = []
    all_providers = []
    
    url = "https://web.khda.gov.ae/en/Education-Directory/Training"
    
    print("Starting browser...")
    browser = None  # Define browser here so it's accessible in finally block
    try:
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=False)  # Set headless=True in production
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
            )
            
            # Add headers to look more like a real browser
            await context.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
            })
            
            page = await context.new_page()
            
            # Set default timeout for all operations
            page.set_default_timeout(60000)  # 60 seconds
            
            # Navigate to the directory page with retry logic
            print(f"Navigating to {url}...")
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await page.goto(url, timeout=60000)
                    print("Waiting for page load...")
                    await page.wait_for_load_state("domcontentloaded", timeout=60000)
                    await page.wait_for_load_state("networkidle", timeout=60000)
                    
                    # Wait for any content to appear - updated selectors based on the current website structure
                    print("Waiting for content to load...")
                    # More comprehensive selectors to find cards, headings, and content items
                    await page.wait_for_selector('table, .directory-item, tr[role="row"], .card, .item, [class*="list-item"], h2, h3, h4, a[href*="details"], div[class*="card"], div[class*="item"]', timeout=60000)
                    
                    # Updated selectors for finding pagination info
                    selectors = [
                        '.k-pager-info.k-label',
                        '.pagination-info',
                        'div[class*="pager"]',
                        'div[class*="pagination"]',
                        '.paginator-info',
                        '.page-count',
                        'span[class*="count"]'
                    ]
                    
                    total_items_text = None
                    for selector in selectors:
                        try:
                            element = await page.query_selector(selector)
                            if element:
                                total_items_text = await element.text_content()
                                print(f"Found pagination info: {total_items_text}")
                                break
                        except Exception:
                            continue
                    
                    if total_items_text:
                        break
                    else:
                        # If no pagination info found, try to count items directly
                        try:
                            items = await page.query_selector_all('tr[role="row"], .directory-item, .card, .item, [class*="list-item"], div[class*="card"], div[class*="item"]')
                            if items:
                                total_items_text = f"1 - {len(items)} of {len(items)} items"
                                print(f"Estimated pagination info: {total_items_text}")
                                break
                        except Exception:
                            pass
                        
                        print("Could not find pagination info, will continue anyway")
                        total_items_text = "unknown number of items"
                        break
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise Exception(f"Failed to load page after {max_retries} attempts: {str(e)}")
                    print(f"Attempt {attempt + 1} failed, retrying in 5 seconds...")
                    await asyncio.sleep(5)
            
            # Skip screenshots as we only need CSV data
            
            total_items_match = re.search(r'of (\d+) items', total_items_text)
            total_items = int(total_items_match.group(1)) if total_items_match else 0
            print(f"Found {total_items} training providers")
            
            # Process providers one by one
            page_num = 1
            while True:
                print(f"Processing page {page_num}")
                
                # Find all the provider name links directly and get their info immediately
                provider_links = await page.query_selector_all('a[id="lnkName"]')
                print(f"Found {len(provider_links)} provider links on page {page_num}")
                
                # Process each provider link directly
                # Use numeric index to keep track of where we are
                provider_index = 0
                while provider_index < len(provider_links):
                    try:
                        link = provider_links[provider_index]
                        name = await link.inner_text()
                        href = await link.get_attribute('href')
                        
                        # Extract area and location information from the listing page card
                        area = "N/A"
                        location = "N/A"
                        
                        try:
                            # Find the parent container/card of this link
                            parent_card = await link.evaluate("""
                                (element) => {
                                    // Look for the closest parent that contains all the card information
                                    let parent = element.closest('tr') || 
                                               element.closest('.card') || 
                                               element.closest('[class*="item"]') ||
                                               element.closest('div');
                                    return parent;
                                }
                            """)
                            
                            if parent_card:
                                # Get the parent element handle
                                parent_handle = await page.evaluate_handle("""
                                    (link) => {
                                        return link.closest('tr') || 
                                               link.closest('.card') || 
                                               link.closest('[class*="item"]') ||
                                               link.closest('div');
                                    }
                                """, link)
                                
                                if parent_handle:
                                    # Extract all text content from the parent card
                                    card_text = await parent_handle.inner_text()
                                    
                                    # Split into lines and analyze the structure
                                    lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                                    
                                    # Try to identify area and location patterns
                                    # The area is usually the line right after the provider name
                                    # Location is usually after a "Location" label
                                    
                                    name_index = -1
                                    for i, line in enumerate(lines):
                                        if name.strip().lower() in line.lower():
                                            name_index = i
                                            break
                                    
                                    if name_index >= 0:
                                        # Area is typically the next line after the name
                                        if name_index + 1 < len(lines):
                                            potential_area = lines[name_index + 1]
                                            # Check if this looks like an area (not "Location" or other headers)
                                            if not potential_area.lower().startswith('location') and len(potential_area) > 2:
                                                area = potential_area
                                    
                                    # Look for location information
                                    for i, line in enumerate(lines):
                                        if line.lower().strip() == 'location' and i + 1 < len(lines):
                                            location = lines[i + 1]
                                            break
                                        elif line.lower().startswith('location:'):
                                            location = line.replace('location:', '').strip()
                                            break
                                        elif 'location' in line.lower() and len(line) > 10:
                                            # Sometimes location is in the same line
                                            location = line
                                            break
                                    
                                    # Alternative approach: look for specific selectors within the card
                                    try:
                                        # Try to find area using common patterns
                                        area_selectors = [
                                            'td:nth-child(2)',  # Second column in table
                                            '.area',
                                            '.location-area',
                                            '[class*="area"]'
                                        ]
                                        
                                        for selector in area_selectors:
                                            try:
                                                area_elem = await parent_handle.query_selector(selector)
                                                if area_elem:
                                                    area_text = await area_elem.inner_text()
                                                    if area_text and area_text.strip() and area == "N/A":
                                                        area = area_text.strip()
                                                        break
                                            except:
                                                continue
                                        
                                        # Try to find location using common patterns
                                        location_selectors = [
                                            'td:nth-child(3)',  # Third column in table
                                            '.location',
                                            '.address',
                                            '[class*="location"]',
                                            '[class*="address"]'
                                        ]
                                        
                                        for selector in location_selectors:
                                            try:
                                                location_elem = await parent_handle.query_selector(selector)
                                                if location_elem:
                                                    location_text = await location_elem.inner_text()
                                                    if location_text and location_text.strip() and location == "N/A":
                                                        location = location_text.strip()
                                                        break
                                            except:
                                                continue
                                                
                                    except Exception as e:
                                        print(f"Error in alternative selector approach: {e}")
                                        
                        except Exception as e:
                            print(f"Error extracting area/location for {name}: {e}")
                        
                        provider_index += 1  # Increment index after getting details
                        
                        print(f"\nProcessing provider {provider_index}/{len(provider_links)}: {name}")
                        print(f"  Area: {area}")
                        print(f"  Listing Location: {location}")
                        
                        # Navigate directly to the detail page using the href
                        if href:
                            if href.startswith('/'):
                                detail_url = f"https://web.khda.gov.ae{href}"
                            else:
                                detail_url = f"https://web.khda.gov.ae/en/Education-Directory/{href}"
                            
                            await page.goto(detail_url)
                            await page.wait_for_load_state("networkidle")
                            await asyncio.sleep(2)
                        else:
                            print(f"No href found for {name}, skipping")
                            continue
                        
                        # Extract detailed information from the detail page
                        detailed_data = {
                            'name': name,
                            'area': area,
                            'listing_location': location
                        }
                        
                        # Extract website
                        website = "N/A"
                        website_selectors = [
                            'a[href*="http"]:not([href*="khda.gov.ae"])',  # External links only
                            'a[target="_blank"]',
                            'a[href*=".com"]',
                            'a[href*=".ae"]:not([href*="khda.gov.ae"])'
                        ]
                        
                        for selector in website_selectors:
                            try:
                                website_elem = await page.query_selector(selector)
                                if website_elem:
                                    href_attr = await website_elem.get_attribute('href')
                                    if href_attr and "http" in href_attr and "khda.gov.ae" not in href_attr:
                                        website = href_attr
                                        break
                            except Exception:
                                continue
                        detailed_data['website'] = website
                        
                        # Extract email
                        email = "N/A"
                        email_selectors = [
                            'a[href*="mailto:"]',
                            'a[href*="@"]'
                        ]
                        
                        for selector in email_selectors:
                            try:
                                email_elem = await page.query_selector(selector)
                                if email_elem:
                                    href_attr = await email_elem.get_attribute('href')
                                    if href_attr and '@' in href_attr:
                                        email = href_attr.replace('mailto:', '').strip()
                                        break
                            except Exception:
                                continue
                        
                        # Also try to find email in text content
                        if email == "N/A":
                            try:
                                page_content = await page.text_content('body')
                                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                                emails_found = re.findall(email_pattern, page_content)
                                if emails_found:
                                    email = emails_found[0]
                            except Exception:
                                pass
                        
                        detailed_data['email'] = email
                        
                        # Extract phone
                        phone = "N/A"
                        phone_selectors = [
                            'a[href*="tel:"]'
                        ]
                        
                        for selector in phone_selectors:
                            try:
                                phone_elem = await page.query_selector(selector)
                                if phone_elem:
                                    href_attr = await phone_elem.get_attribute('href')
                                    if href_attr:
                                        phone = href_attr.replace('tel:', '').strip()
                                        break
                            except Exception:
                                continue
                        
                        # Also try to find phone in text content
                        if phone == "N/A":
                            try:
                                page_content = await page.text_content('body')
                                phone_pattern = r'(\+?[0-9\s\-\(\)]{7,15})'
                                phones_found = re.findall(phone_pattern, page_content)
                                if phones_found:
                                    phone = phones_found[0].strip()
                            except Exception:
                                pass
                        
                        detailed_data['phone'] = phone
                        
                        # Extract address
                        address = "N/A"
                        try:
                            address_selectors = [
                                '[class*="address"]',
                                '[class*="location"]',
                                'address'
                            ]
                            
                            for selector in address_selectors:
                                try:
                                    address_elem = await page.query_selector(selector)
                                    if address_elem:
                                        address_text = await address_elem.inner_text()
                                        if address_text and len(address_text.strip()) > 5:
                                            address = address_text.strip()
                                            break
                                except Exception:
                                    continue
                        except Exception:
                            pass
                        
                        detailed_data['address'] = address
                        
                        # Extract programs offered from the specific accordion
                        program_structure = {}  # Dictionary to store main programs and their sub-programs
                        flat_programs = []  # For backward compatibility
                        
                        print("Looking for Programs Offered accordion...")
                        
                        # Make sure the page is stable before interacting with accordions
                        await page.wait_for_load_state("networkidle")
                        await page.wait_for_timeout(1000)  # Additional wait to ensure page stability
                        
                        # Use our helper function to find and expand the accordion
                        try:
                            accordion_content = await click_and_expand_accordion(page, "Programs Offered")
                        except Exception as e:
                            print(f"Error accessing Programs Offered accordion: {e}")
                            accordion_content = None
                        
                        if accordion_content:
                            print("Found and expanded Programs Offered accordion")
                            
                            # First, try to identify main program headings
                            main_program_selectors = [
                                'h3', 'h4', 'h5', 'strong', '.program-title', '.main-program',
                                'div[class*="header"]', '.accordion-button', '.card-header',
                                'div[role="button"]', 'a[role="button"]', '.panel-heading',
                                '.accordion-header', 'button[data-toggle="collapse"]',
                                '[class*="accordion"]', 'a[data-toggle="tab"]', '[class*="tab-header"]',
                                '.nav-link', '[data-bs-toggle="collapse"]', '[data-bs-toggle="tab"]',
                                '.btn-accordion', '.toggle-trigger', '.dropdown-toggle'
                            ]
                            
                            main_programs = []
                            for selector in main_program_selectors:
                                try:
                                    print(f"Searching for main programs with selector: {selector}")
                                    elements = await accordion_content.query_selector_all(selector)
                                    if elements and len(elements) > 0:
                                        for element in elements:
                                            text = await element.inner_text()
                                            if text and len(text.strip()) > 1:
                                                # Clean up the text
                                                title = text.strip()
                                                
                                                # Check if we already have this title
                                                if not any(p['title'] == title for p in main_programs):
                                                    main_programs.append({
                                                        'title': title,
                                                        'element': element
                                                    })
                                        
                                        if main_programs:
                                            print(f"Found {len(main_programs)} potential main programs using selector: {selector}")
                                            
                                            # If we found a good number of programs, stop looking
                                            if len(main_programs) > 1:
                                                break
                                except Exception as e:
                                    print(f"Error with selector '{selector}': {e}")
                            
                            # If we couldn't find any main programs, try looking for any clickable elements
                            if not main_programs:
                                print("No main programs found with standard selectors, trying alternative approach")
                                try:
                                    # Look for any clickable elements that might be program headers
                                    clickable_elements = await page.evaluate("""
                                        () => {
                                            const possibleHeaders = [];
                                            // Look for elements with click handlers or that look like headers
                                            document.querySelectorAll('*').forEach(el => {
                                                if (
                                                    // Has click handler
                                                    (el.onclick || el.getAttribute('onclick')) ||
                                                    // Looks like a button
                                                    el.tagName === 'BUTTON' || 
                                                    el.tagName === 'A' ||
                                                    // Has role attribute
                                                    el.getAttribute('role') === 'button' ||
                                                    // Has expandable attributes
                                                    el.getAttribute('aria-expanded') !== null ||
                                                    // Has Bootstrap collapse attributes
                                                    el.getAttribute('data-toggle') === 'collapse' ||
                                                    el.getAttribute('data-bs-toggle') === 'collapse'
                                                ) {
                                                    const text = el.innerText?.trim();
                                                    if (text && text.length > 1) {
                                                        const rect = el.getBoundingClientRect();
                                                        // Only include visible elements with some width and height
                                                        if (rect.width > 0 && rect.height > 0) {
                                                            possibleHeaders.push({
                                                                text: text,
                                                                selector: el.id ? `#${el.id}` : null,
                                                                className: el.className,
                                                                tagName: el.tagName,
                                                                index: Array.from(document.querySelectorAll(el.tagName)).indexOf(el)
                                                            });
                                                        }
                                                    }
                                                });
                                            });
                                            return possibleHeaders;
                                        }
                                    """)
                                    
                                    if clickable_elements:
                                        print(f"Found {len(clickable_elements)} potential clickable program headers")
                                        
                                        # Try to get handles to these elements
                                        for item in clickable_elements:
                                            try:
                                                selector = None
                                                
                                                # Try to construct a selector to find this element
                                                if item['selector']:
                                                    selector = item['selector']
                                                elif item['className'] and item['tagName']:
                                                    selector = f"{item['tagName'].toLowerCase()}.{item['className'].replace(' ', '.')}"

                                                elif item['tagName'] and item['index'] is not None:
                                                    selector = f"//{ item['tagName'].toLowerCase() }[{ item['index'] + 1 }]"


                                                if selector:
                                                    try:
                                                        element = await page.wait_for_selector(selector, timeout=1000)
                                                        if element:
                                                            main_programs.append({
                                                                'title': item['text'],
                                                                'element': element
                                                            })
                                                    except:
                                                        # Try XPath if CSS selector fails
                                                        if selector.startswith('//'):
                                                            try:
                                                                element = await page.wait_for_selector(selector, timeout=1000)
                                                                if element:
                                                                    main_programs.append({
                                                                        'title': item['text'],
                                                                        'element': element
                                                                    })
                                                            except:
                                                                pass
                                            except Exception as e:
                                                print(f"Error processing clickable element: {e}")
                                except Exception as e:
                                    print(f"Error in alternative approach: {e}")
                        
                            # If we found main programs, try to extract their sub-programs
                            if main_programs:
                                for idx, main_program in enumerate(main_programs):
                                    try:
                                        main_title = main_program['title']
                                        elem = main_program['element']
                                        
                                        print(f"Processing main program: {main_title}")
                                        
                                        # Use our specialized helper function to click and extract subprograms
                                        sub_programs = await click_and_extract_subprograms(page, elem, main_title)                                            # Clean up subprograms - remove duplicates, filter out noise
                                        if sub_programs:
                                            clean_subs = []
                                            # List of common navigation elements and non-program items to filter out
                                            nav_elements = [
                                                'home', 'about', 'contact', 'menu', 'skip to content', 
                                                'services', 'about us', 'find', 'resources', 'guides', 
                                                'participate', 'search', 'login', 'register', 'sign in',
                                                'education institutions', 'find education', 'copyright',
                                                'privacy', 'terms', 'sitemap', 'faq', 'help',
                                                'click', 'toggle', 'close', 'open', 'show', 'hide', 
                                                'programs offered', 'next', 'previous', 'submit', 
                                                'apply now', 'learn more', 'read more', 'view all'
                                            ]
                                            
                                            # Find duplicate text patterns to detect repeated navigation elements
                                            duplicate_count = {}
                                            for sub in sub_programs:
                                                if sub and sub.strip():
                                                    clean_sub = sub.strip()
                                                    duplicate_count[clean_sub] = duplicate_count.get(clean_sub, 0) + 1
                                            
                                            # Identify items that appear 3+ times (likely navigation elements)
                                            repeated_elements = [text for text, count in duplicate_count.items() if count >= 3]
                                            
                                            # Process each subprogram
                                            for sub in sub_programs:
                                                # Skip items that are empty or very short
                                                if not sub or len(sub.strip()) < 3:
                                                    continue
                                                
                                                clean_sub = sub.strip()
                                                
                                                # Skip if in repeated elements list (likely navigation)
                                                if clean_sub in repeated_elements:
                                                    continue
                                                    
                                                # Skip common navigation elements
                                                if any(nav.lower() == clean_sub.lower() or 
                                                      (len(nav) > 4 and nav.lower() in clean_sub.lower()) 
                                                      for nav in nav_elements):
                                                    continue
                                                    
                                                # Skip if this is identical to main program
                                                if clean_sub == main_title.strip():
                                                    continue
                                                
                                                # Skip if it's a URL or contains http
                                                if 'http' in clean_sub.lower() or 'www.' in clean_sub.lower():
                                                    continue
                                                
                                                # Skip if it looks like a date, time, or just numbers
                                                if re.search(r'\d{2}[:/]\d{2}[:/]\d{2,4}', clean_sub) or re.match(r'^[\d\s\.,]+$', clean_sub):
                                                    continue
                                                    
                                                # Skip if it's too long (likely paragraph content, not a program name)
                                                if len(clean_sub) > 100:
                                                    # Try to extract just the first line which might be the program name
                                                    first_line = clean_sub.split('\n')[0].strip()
                                                    if first_line and len(first_line) < 100 and not any(nav.lower() in first_line.lower() for nav in nav_elements):
                                                        clean_sub = first_line
                                                    else:
                                                        continue
                                                
                                                # Skip if it's too similar to something we already have
                                                # (this helps remove slight variations of the same program)
                                                similar_exists = False
                                                for existing in clean_subs:
                                                    # Check for high similarity using Levenshtein distance
                                                    if len(existing) > 0 and len(clean_sub) > 0:
                                                        # If one string contains the other completely
                                                        if existing.lower() in clean_sub.lower() or clean_sub.lower() in existing.lower():
                                                            similar_exists = True
                                                            break
                                                
                                                # Add to clean list if not a duplicate and valid
                                                if clean_sub and not similar_exists and clean_sub not in clean_subs:
                                                    clean_subs.append(clean_sub)
                                                
                                            program_structure[main_title] = clean_subs
                                            print(f"  Found {len(clean_subs)} sub-programs for {main_title}")
                                            
                                            # Also add to flat list for backward compatibility
                                            for sub in clean_subs:
                                                flat_programs.append(f"{main_title} - {sub}")
                                        else:
                                            # If no sub-programs found, treat the main program as a standalone program
                                            program_structure[main_title] = []
                                            flat_programs.append(main_title)
                                            print(f"  No sub-programs found for {main_title}")
                                    except Exception as e:
                                        print(f"Error processing main program {main_title}: {e}")
                            

                            # If no main programs were found, fall back to the original approach
                            if not program_structure:
                                print("No main programs found, using original extraction approach")
                                
                                # Try to parse content text to identify potential main programs and sub-programs
                                try:
                                    content_text = await accordion_content.inner_text()
                                    if content_text:
                                        # Split by line breaks and clean up
                                        lines = [line.strip() for line in content_text.split('\n') if line.strip()]
                                        
                                        # Try to identify patterns that might indicate main programs vs sub-programs
                                        current_main_program = None
                                        for line in lines:
                                            # Potential indicators of main programs:
                                            # - Short lines with specific capitalization patterns
                                            # - Lines ending with a colon
                                            # - Lines with fewer than 5 words and all words capitalized or title case
                                            # - Lines that start with numbers like "1." or "1)"
                                            
                                            is_main_program = False
                                            
                                            # Check for numbered list items that could be main programs
                                            if re.match(r'^\d+[\.\)]\s+\w', line):
                                                is_main_program = True
                                            # Check for lines ending with colon
                                            elif line.endswith(':'):
                                                is_main_program = True
                                            # Check for short, prominently formatted lines (likely titles)
                                            elif len(line.split()) < 5 and (line.isupper() or line[0].isupper()):
                                                is_main_program = True
                                            # Check for lines that look like headings (all words capitalized)
                                            elif all(word[0].isupper() for word in line.split() if word):
                                                is_main_program = True
                                                
                                            if is_main_program:
                                                # This line looks like a main program
                                                current_main_program = line.strip().rstrip(':')
                                                program_structure[current_main_program] = []
                                            elif current_main_program and line:
                                                # This line is likely a sub-program of the current main program
                                                program_structure[current_main_program].append(line.strip())
                                        
                                        # If we identified structure, create the flat_programs list
                                        if program_structure:
                                            print(f"Identified {len(program_structure)} main programs from text patterns")
                                            for main, subs in program_structure.items():
                                                if subs:
                                                    for sub in subs:
                                                        flat_programs.append(f"{main} - {sub}")
                                                else:
                                                    flat_programs.append(main)
                                except Exception as e:
                                    print(f"Error trying to parse structure from text: {e}")
                                    
                                # If we still don't have structure, fall back to the original flat approach
                                if not flat_programs:
                                    print("Falling back to basic extraction approach")
                                    # Try different selectors to extract program information
                                    for selector in [
                                        'li',  # List items inside the accordion
                                        '.list-item', 
                                        'p',   # Paragraph tags
                                        'div[class*="program"]',
                                        'div[class*="item"]',
                                        '.card',
                                        'span'  # Fallback to any span
                                    ]:
                                        try:
                                            program_items = await accordion_content.query_selector_all(selector)
                                            if program_items and len(program_items) > 0:
                                                for prog in program_items:
                                                    prog_text = await prog.inner_text()
                                                    if prog_text and len(prog_text.strip()) > 1:  # Ignore empty items
                                                        flat_programs.append(prog_text.strip())
                                                
                                                if flat_programs:
                                                    print(f"Found {len(flat_programs)} programs using selector: {selector}")
                                                    break
                                        except Exception as e:
                                            print(f"Error extracting programs with selector {selector}: {e}")
                        
                        # Skipping all screenshot operations as we only need CSV data
                        
                        # Clean up the extracted programs list
                        if flat_programs:
                            # Remove duplicates while preserving order
                            unique_programs = []
                            for prog in flat_programs:
                                prog_clean = prog.strip()
                                # Skip empty strings or very short items (likely just formatting)
                                if prog_clean and len(prog_clean) > 2 and prog_clean not in unique_programs:
                                    unique_programs.append(prog_clean)
                            
                            # Further filter out common non-program text
                            filtered_programs = []
                            non_program_keywords = ['home', 'about us', 'contact', 'menu', 'collapse', 'expand', 
                                                  'click', 'toggle', 'close', 'open', 'show', 'hide', 'programs offered']
                            
                            for prog in unique_programs:
                                # Skip items that are just one of our non-program keywords
                                if prog.lower() not in non_program_keywords:
                                    filtered_programs.append(prog)
                            
                            # Use the unified 'programs' field instead of programs_offered
                            if not program_structure:  # Only set if we don't have structured programs
                                detailed_data['programs'] = '; '.join(filtered_programs) if filtered_programs else "N/A"
                        else:
                            # Only set if we don't already have programs data
                            if 'programs' not in detailed_data:
                                detailed_data['programs'] = "N/A"
                        
                        # Store the structured program data
                        if program_structure:
                            # Format the program structure into a single, well-formatted string
                            formatted_programs = []
                            for main_program, sub_programs in program_structure.items():
                                if sub_programs and len(sub_programs) > 0:
                                    # Filter out any navigation elements or empty strings
                                    filtered_subs = [sub for sub in sub_programs 
                                                   if sub and len(sub.strip()) > 2 
                                                   and not any(nav in sub.lower() for nav in ['home', 'about', 'contact', 'menu'])]
                                    
                                    if filtered_subs:
                                        sub_list = ", ".join(filtered_subs)
                                        formatted_programs.append(f"{main_program} ({sub_list})")
                                    else:
                                        formatted_programs.append(main_program)
                                else:
                                    formatted_programs.append(main_program)
                            
                            # Store only in one column to avoid clutter
                            detailed_data['programs'] = '; '.join(formatted_programs) if formatted_programs else "N/A"
                        else:
                            detailed_data['programs'] = detailed_data.get('programs_offered', "N/A")
                            
                        # Remove legacy columns to avoid duplication
                        if 'programs_offered' in detailed_data:
                            del detailed_data['programs_offered']
                        if 'formatted_programs' in detailed_data:
                            del detailed_data['formatted_programs']
                        if 'program_structure' in detailed_data:
                            del detailed_data['program_structure']
                        
                        # Add to our lists
                        all_providers.append(detailed_data)
                        detailed_providers.append(detailed_data)
                        
                        print(f"Extracted data for: {name}")
                        print(f"  Area: {area}")
                        print(f"  Listing Location: {location}")
                        print(f"  Website: {website}")
                        print(f"  Email: {email}")
                        print(f"  Phone: {phone}")
                        print(f"  Detail Address: {address}")
                        print(f"  Programs: {detailed_data['programs'][:100]}...")
                        
                        # Save this record immediately to CSV
                        try:
                            # Make a copy of the data to ensure any serialization issues don't affect the original
                            save_data = detailed_data.copy()
                            
                            # Before saving, ensure we only have the consolidated 'programs' field
                            # Remove the old columns if they exist
                            if 'programs_offered' in save_data:
                                del save_data['programs_offered']
                            if 'program_structure' in save_data:
                                del save_data['program_structure']
                            if 'formatted_programs' in save_data:
                                del save_data['formatted_programs']
                                
                            # Make sure the programs data is clean
                            if 'programs' in save_data and save_data['programs']:
                                # Clean up the programs to remove any navigation elements
                                nav_elements = [
                                    'home', 'about', 'contact', 'menu', 'skip to content', 
                                    'services', 'about us', 'find', 'resources', 'guides', 
                                    'participate', 'search', 'login', 'register', 'sign in',
                                    'education institutions', 'find education', 'copyright',
                                    'privacy', 'terms', 'sitemap', 'faq', 'help'
                                ]
                                
                                # Split the programs string, clean each item, and rejoin
                                cleaned_programs = []
                                for prog_item in save_data['programs'].split(';'):
                                    prog_item = prog_item.strip()
                                    # Skip if the item is a navigation element
                                    if any(nav.lower() in prog_item.lower() for nav in nav_elements):
                                        continue
                                    if prog_item and prog_item not in cleaned_programs:
                                        cleaned_programs.append(prog_item)
                                
                                # Update with cleaned data
                                if cleaned_programs:
                                    save_data['programs'] = '; '.join(cleaned_programs)
                                else:
                                    save_data['programs'] = "N/A"
                            
                            # Create DataFrame from the processed data
                            df = pd.DataFrame([save_data])
                            
                            # Ensure results directory exists
                            os.makedirs('results', exist_ok=True)
                            
                            # Save to CSV with proper error handling
                            csv_path = 'results/khda_training_providers_detailed.csv'
                            
                            # Use a more robust approach with retry logic for saving to CSV
                            max_retries = 5
                            retry_count = 0
                            saved = False
                            
                            while retry_count < max_retries and not saved:
                                try:
                                    # Check if file exists to determine if we need to write headers
                                    file_exists = os.path.isfile(csv_path)
                                    
                                    # Generate a unique temporary file name
                                    import time
                                    import uuid
                                    temp_csv_path = f'results/temp_{uuid.uuid4().hex}_{int(time.time())}.csv'
                                    
                                    if not file_exists:
                                        # For new file, write directly with headers
                                        df.to_csv(csv_path, index=False, encoding='utf-8', lineterminator='\n')
                                        print(f"Created new CSV file: {csv_path}")
                                        saved = True
                                    else:
                                        # For existing file, try to update properly
                                        try:
                                            # First load existing data
                                            existing_df = None
                                            try:
                                                existing_df = pd.read_csv(csv_path)
                                            except Exception as read_err:
                                                print(f"Warning: Could not read existing CSV: {read_err}")
                                                
                                            if existing_df is not None and not existing_df.empty and 'name' in existing_df.columns:
                                                # Check if this provider already exists
                                                if save_data['name'] in existing_df['name'].values:
                                                    print(f"Provider {save_data['name']} already exists in CSV, updating...")
                                                    # Update the existing record
                                                    existing_df.loc[existing_df['name'] == save_data['name']] = df.iloc[0]
                                                    existing_df.to_csv(temp_csv_path, index=False, encoding='utf-8', lineterminator='\n')
                                                else:
                                                    # Append to existing data
                                                    updated_df = pd.concat([existing_df, df], ignore_index=True)
                                                    updated_df.to_csv(temp_csv_path, index=False, encoding='utf-8', lineterminator='\n')
                                                
                                                # Safely replace the original file
                                                import shutil
                                                if os.path.exists(temp_csv_path):
                                                    try:
                                                        # In Windows, we may need to remove the target file first
                                                        if os.path.exists(csv_path):
                                                            os.remove(csv_path)
                                                        shutil.move(temp_csv_path, csv_path)
                                                        saved = True
                                                        print(f"Successfully updated CSV file: {csv_path}")
                                                    except Exception as move_err:
                                                        print(f"Error moving temp file: {move_err}")
                                            else:
                                                # If we couldn't read or process the existing file, 
                                                # use a provider-specific file as a fallback
                                                provider_csv = f'results/provider_{save_data["name"].replace(" ", "_").replace("/", "_")}.csv'
                                                df.to_csv(provider_csv, index=False, encoding='utf-8', lineterminator='\n')
                                                print(f"Created provider-specific CSV: {provider_csv}")
                                                saved = True
                                        except Exception as process_err:
                                            print(f"Error processing CSV data: {process_err}")
                                            retry_count += 1
                                except PermissionError as e:
                                    print(f"Permission error (attempt {retry_count+1}/{max_retries}): {e}")
                                    retry_count += 1
                                    time.sleep(1)  # Wait before retry
                                except Exception as e:
                                    print(f"Unexpected error saving CSV: {e}")
                                    retry_count += 1
                                    time.sleep(1)  # Wait before retry
                        except Exception as e:
                            print(f"Error saving to CSV: {str(e)}")
                            # Try an alternative approach for saving if the primary method fails
                            try:
                                print("Attempting alternative save method...")
                                
                                # Make sure results directory exists
                                if not os.path.exists('results'):
                                    try:
                                        os.makedirs('results', exist_ok=True)
                                        print("Created results directory")
                                    except Exception as mkdir_err:
                                        print(f"Error creating results directory: {mkdir_err}")
                                        # Try to use the current directory instead
                                        results_dir = '.'
                                
                                # Create a safe filename
                                safe_filename = ''.join(c if c.isalnum() else '_' for c in name)
                                backup_path = f'results/provider_{safe_filename}.txt'
                                
                                # Check if we have write permissions in the directory
                                try:
                                    with open(backup_path, 'w') as test_file:
                                        pass
                                    os.remove(backup_path)
                                    print("Directory is writable")
                                except Exception as perm_err:
                                    print(f"Warning: Directory may not be writable: {perm_err}")
                                    backup_path = f'provider_{safe_filename}.txt'  # Try in current directory
                                
                                # Save to text file
                                with open(backup_path, 'w') as f:
                                    f.write(f"Provider: {name}\n")
                                    f.write(f"Website: {website}\n")
                                    f.write(f"Email: {email}\n")
                                    f.write(f"Phone: {phone}\n")
                                    f.write(f"Programs: {detailed_data.get('programs', 'N/A')}\n")
                                    
                                    # Add debug information to help diagnose issues
                                    f.write("\nDebug Information:\n")
                                    f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                                    f.write(f"Error encountered: {str(e)}\n")
                                    
                                print(f"Saved minimal data as text file to {backup_path}")
                                
                                # Try a separate CSV file just for this provider
                                try:
                                    provider_csv = f'results/single_provider_{safe_filename}.csv'
                                    df = pd.DataFrame([detailed_data])
                                    df.to_csv(provider_csv, index=False, encoding='utf-8', lineterminator='\n')
                                    print(f"Also saved as individual CSV: {provider_csv}")
                                except Exception as csv_err:
                                    print(f"Could not save individual CSV: {csv_err}")
                                    
                            except Exception as backup_err:
                                print(f"Backup save method also failed: {backup_err}")
                                # Last resort - print the data to console
                                print("\nProvider data that could not be saved:")
                                for key, value in detailed_data.items():
                                    print(f"  {key}: {value}")
                        
                        # Go back to the main listing by creating a completely new page
                        print("Going back to main listing...")
                        try:
                            # Close the current page and create a new one to avoid context issues
                            await page.close()
                            page = await context.new_page()
                            
                            # Set default timeout for all operations on new page
                            page.set_default_timeout(60000)  # 60 seconds
                            
                            # Navigate to main URL
                            await page.goto(url)
                            await page.wait_for_load_state("networkidle")
                            await page.wait_for_timeout(2000)  # Additional wait for stability
                            
                            # If we're not on page 1, navigate to the correct page
                            if page_num > 1:
                                print(f"Navigating back to page {page_num}...")
                                # Navigate to the correct page number
                                for p in range(1, page_num):
                                    await asyncio.sleep(1)  # Wait a bit between clicks
                                    
                                    # Find and click next button
                                    next_clicked = False
                                    for selector in [
                                        'a.k-link[title="Go to the next page"]',
                                        '.k-pager-nav.k-link.k-pager-next:not(.k-state-disabled)',
                                        'button[aria-label="Next page"]', 
                                        '[aria-label="next page"]',
                                        '[class*="next"]:not([disabled])',
                                        'a[class*="next"]', 
                                        'button[class*="next"]',
                                        'a:has-text("Next")',
                                        'button:has-text("Next")'
                                    ]:
                                        try:
                                            # Use waitForSelector with state attached to ensure element exists
                                            next_btn = await page.wait_for_selector(selector, timeout=2000, state='attached')
                                            if next_btn:
                                                # Use JavaScript click to avoid ElementHandle issues
                                                await page.evaluate("(btn) => btn.click()", next_btn)
                                                await page.wait_for_load_state("networkidle")
                                                await page.wait_for_timeout(2000)  # Add additional wait after navigation
                                                next_clicked = True
                                                break
                                        except Exception:
                                            continue
                                    
                                    if not next_clicked:
                                        print(f"Could not navigate to page {p+1}, staying on current page")
                                        break
                            
                            # Once back on the correct page, refresh provider links
                            provider_links = await page.query_selector_all('a[id="lnkName"]')
                            print(f"Found {len(provider_links)} provider links after navigation")
                            
                        except Exception as e:
                            print(f"Error navigating back to main listing page: {e}")
                            # Try a simple navigation as a fallback with a new page
                            try:
                                await page.close()
                                page = await context.new_page()
                                page.set_default_timeout(60000)
                                await page.goto(url)
                                await page.wait_for_load_state("networkidle")
                                await page.wait_for_timeout(2000)
                                
                                # Refresh provider links
                                provider_links = await page.query_selector_all('a[id="lnkName"]')
                            except Exception as nav_error:
                                print(f"Critical navigation error: {nav_error}")
                                # If we can't navigate back, we should break out of the loop
                                raise
                        
                    except Exception as e:
                        print(f"Error processing provider {name}: {str(e)}")
                        # No need to decrement provider_index as we've already incremented it
                        # and want to move on to the next provider
                        
                        # Create a new page to avoid context issues
                        try:
                            await page.close()
                            page = await context.new_page()
                            page.set_default_timeout(60000)
                            
                            # Go back to the main listing
                            await page.goto(url)
                            await page.wait_for_load_state("networkidle")
                            await page.wait_for_timeout(2000)
                            
                            # If we're not on page 1, navigate to the correct page
                            if page_num > 1:
                                for p in range(1, page_num):
                                    # Find and click next button
                                    for selector in [
                                        'a.k-link[title="Go to the next page"]',
                                        '.k-pager-nav.k-pager-next:not(.k-state-disabled)',
                                        'button[aria-label="Next page"]',
                                        'a:has-text("Next")'
                                    ]:
                                        try:
                                            next_btn = await page.wait_for_selector(selector, timeout=2000)
                                            if next_btn:
                                                await page.evaluate("(btn) => btn.click()", next_btn)
                                                await page.wait_for_load_state("networkidle")
                                                await page.wait_for_timeout(2000)
                                                break
                                        except Exception:
                                            continue
                            
                            # Refresh provider links
                            provider_links = await page.query_selector_all('a[id="lnkName"]')
                            print(f"Found {len(provider_links)} provider links after error recovery")
                            
                            # Adjust provider_index if necessary to avoid skipping or double-processing
                            if provider_index > len(provider_links):
                                provider_index = len(provider_links)
                        except Exception as page_e:
                            print(f"FATAL: Could not navigate back to main page: {page_e}. Aborting.")
                            break
                        continue
                # After processing all providers on the current page, check for next page
                # Only proceed to next page if we've processed all providers on the current page
                if provider_index >= len(provider_links):
                    # Check if there's a next page using multiple potential selectors
                    next_button = None
                    has_next_page = False
                    
                    for selector in [
                        'a.k-link[title="Go to the next page"]',
                        '.k-pager-nav.k-pager-next:not(.k-state-disabled)',
                        'button[aria-label="Next page"], [aria-label="next page"]',
                        '[class*="next"]:not([disabled])',
                        'a[class*="next"], button[class*="next"]',
                        '[aria-label*="next"]',
                        'a:has-text("Next")',
                        'button:has-text("Next")'
                    ]:
                        try:
                            next_button = await page.wait_for_selector(selector, timeout=2000, state='attached')
                            if next_button:
                                # Check if it's enabled using JavaScript
                                is_enabled = await page.evaluate("""
                                    (btn) => {
                                        return !btn.disabled && 
                                               !btn.classList.contains('disabled') && 
                                               !btn.classList.contains('k-state-disabled');
                                    }
                                """, next_button)
                                
                                if is_enabled:
                                    has_next_page = True
                                    break
                        except Exception:
                            continue
                    
                    if has_next_page:
                        print(f"Moving to page {page_num + 1}")
                        # Use JavaScript for the click to avoid ElementHandle issues
                        await page.evaluate("(btn) => btn.click()", next_button)
                        await page.wait_for_load_state("networkidle")
                        await page.wait_for_timeout(3000)  # Longer delay for stability between pages
                        page_num += 1
                        
                        # Refresh provider links for the new page
                        provider_links = await page.query_selector_all('a[id="lnkName"]')
                        print(f"Found {len(provider_links)} provider links on page {page_num}")
                        
                        # Reset provider index for the new page
                        provider_index = 0
                    else:
                        print("No more pages to process or no next button found")
                        break
            print(f"Scraping completed. Collected data for {len(all_providers)} providers")
            
            # Save the data to CSV files
            # Remove 'location' from all_providers and detailed_providers before saving
            for provider in all_providers:
                if 'location' in provider:
                    del provider['location']
            for provider in detailed_providers:
                if 'location' in provider:
                    del provider['location']
            # Ensure results directory exists
            os.makedirs('results', exist_ok=True)
            
            # Save basic data with error handling
            try:
                basic_df = pd.DataFrame(all_providers)
                
                # First save to a temporary file, then rename to avoid permission issues
                temp_basic_file = f'results/temp_basic_{uuid.uuid4().hex}.csv'
                basic_df.to_csv(temp_basic_file, index=False, lineterminator='\n')
                
                # Move the temporary file to the final location
                final_basic_file = 'results/khda_training_providers_basic.csv'
                if os.path.exists(final_basic_file):
                    try:
                        os.remove(final_basic_file)
                    except:
                        pass
                shutil.move(temp_basic_file, final_basic_file)
                print(f"Basic data saved to '{final_basic_file}'")
            except Exception as basic_err:
                print(f"Error saving basic data: {basic_err}")
                # Try alternative location
                try:
                    basic_df.to_csv('khda_training_providers_basic.csv', index=False, lineterminator='\n')
                    print("Basic data saved to current directory")
                except:
                    print("Could not save basic data")

            if detailed_providers:
                try:
                    # Clean up the providers data before final save
                    for provider in detailed_providers:
                        # Remove legacy columns if they exist
                        if 'programs_offered' in provider:
                            del provider['programs_offered']
                        if 'program_structure' in provider:
                            del provider['program_structure']
                        if 'formatted_programs' in provider:
                            del provider['formatted_programs']
                        
                        # Ensure we have a programs field
                        if 'programs' not in provider or not provider['programs']:
                            provider['programs'] = 'N/A'
                    
                    # Create DataFrame and save to CSV using the same safe approach
                    detailed_df = pd.DataFrame(detailed_providers)
                    
                    # First save to a temporary file
                    temp_detailed_file = f'results/temp_detailed_{uuid.uuid4().hex}.csv'
                    detailed_df.to_csv(temp_detailed_file, index=False, lineterminator='\n')
                    
                    # Move the temporary file to the final location
                    final_detailed_file = 'results/khda_training_providers_detailed.csv'
                    if os.path.exists(final_detailed_file):
                        try:
                            os.remove(final_detailed_file)
                        except:
                            pass
                    shutil.move(temp_detailed_file, final_detailed_file)
                    print(f"Detailed data saved to '{final_detailed_file}'")
                except Exception as detailed_err:
                    print(f"Error saving detailed data: {detailed_err}")
                    # Try alternative location
                    try:
                        detailed_df.to_csv('khda_training_providers_detailed.csv', index=False, lineterminator='\n')
                        print("Detailed data saved to current directory")
                    except:
                        print("Could not save detailed data")

            print(f"Basic data saved to 'results/khda_training_providers_basic.csv'")
            if detailed_providers:
                print(f"Detailed data saved to 'results/khda_training_providers_detailed.csv'")
            
            await browser.close()
            return detailed_providers
    except Exception as e:
        print(f"Error during scraping process: {str(e)}")
    finally:
        # Ensure browser is closed in case of error
        try:
            await browser.close()
        except Exception as e:
            print(f"Error closing browser: {str(e)}")
        
        return detailed_providers

# Install Playwright browsers if needed
def setup():
    """
    Ensures all required dependencies are installed.
    """
    try:
        import subprocess
        import sys
        import os
        import json
        import pandas as pd
        import re
        import asyncio
        
        # Check and install required packages
        required_packages = ["playwright", "pandas", "tqdm"]
        for package in required_packages:
            try:
                __import__(package)
                print(f"{package} is already installed.")
            except ImportError:
                print(f"Installing {package}...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        
        # Install Playwright browsers
        print("Installing Playwright browsers...")
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
        
        print("Setup completed successfully.")
    except Exception as e:
        print(f"Error during setup: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Set up dependencies
    setup()
    
    # Run the scraper
    providers = asyncio.run(scrape_training_providers())