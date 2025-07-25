#!/usr/bin/env python3
"""
CLI Interface for Order Cleaning Application
"""
import click
import requests
import json
import os
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Base URL from environment variable
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

def make_request(method: str, endpoint: str, data: Dict = None) -> Dict:
    """Make HTTP request to API"""
    url = f"{API_BASE_URL}{endpoint}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url)
        elif method.upper() == "POST":
            response = requests.post(url, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        click.echo(f"Error making request: {str(e)}")
        return None

@click.group()
def cli():
    """Order Cleaning CLI - Tool untuk memperbaiki data order yang hilang"""
    pass

@cli.command()
@click.option('--start-date', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', required=True, help='End date (YYYY-MM-DD)')
def compare_data(start_date: str, end_date: str):
    """Compare data between Database A and Database B for a given date range"""
    click.echo(f"Comparing data from {start_date} to {end_date}...")
    
    data = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    result = make_request("POST", "/compare-data", data)
    
    if result:
        click.echo(f"\n{result['message']}")
        click.echo(f"Total discrepancies: {result['total_discrepancies']}")
        
        if result['discrepancies']:
            click.echo("\nDiscrepancies found:")
            click.echo("-" * 80)
            click.echo(f"{'DO Number':<20} {'DB A Count':<12} {'DB B Count':<12} {'Difference':<12}")
            click.echo("-" * 80)
            
            for disc in result['discrepancies']:
                click.echo(f"{disc['do_number']:<20} {disc['db_a_count']:<12} {disc['db_b_count']:<12} {disc['discrepancy_count']:<12}")
        else:
            click.echo("No discrepancies found!")

@cli.command()
@click.argument('do_number')
def create_payload(do_number: str):
    """Create payload for a specific do_number"""
    click.echo(f"Creating payload for do_number: {do_number}...")
    
    result = make_request("POST", f"/create-payload/{do_number}")
    
    if result:
        click.echo(f"\n{result['message']}")
        click.echo(f"Status: {result['status']}")
        
        # Save payload to file
        filename = f"payload_{do_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(result['payload_data'], f, indent=2)
        
        click.echo(f"Payload saved to: {filename}")

@cli.command()
@click.option('--limit', default=100, help='Number of results to retrieve')
@click.option('--offset', default=0, help='Number of results to skip')
def list_payloads(limit: int, offset: int):
    """List all payload results"""
    click.echo(f"Retrieving payload results (limit: {limit}, offset: {offset})...")
    
    result = make_request("GET", f"/payload-results?limit={limit}&offset={offset}")
    
    if result:
        click.echo(f"\n{result['message']}")
        
        if result['results']:
            click.echo("\nPayload Results:")
            click.echo("-" * 100)
            click.echo(f"{'ID':<5} {'DO Number':<20} {'Warehouse':<12} {'Client':<10} {'Status':<12} {'Created Date':<20}")
            click.echo("-" * 100)
            
            for payload in result['results']:
                created_date = payload['created_date']
                if created_date:
                    created_date = created_date.split('T')[0]  # Extract date only
                
                click.echo(f"{payload['id']:<5} {payload['do_number']:<20} {payload['warehouse_id']:<12} {payload['client_id']:<10} {payload['status']:<12} {created_date:<20}")
        else:
            click.echo("No payload results found!")

@cli.command()
@click.argument('do_number')
def get_payload(do_number: str):
    """Get specific payload result by do_number"""
    click.echo(f"Retrieving payload result for do_number: {do_number}...")
    
    result = make_request("GET", f"/payload-result/{do_number}")
    
    if result:
        click.echo(f"\nPayload Result for {do_number}:")
        click.echo("-" * 50)
        click.echo(f"ID: {result['id']}")
        click.echo(f"Warehouse ID: {result['warehouse_id']}")
        click.echo(f"Client ID: {result['client_id']}")
        click.echo(f"Status: {result['status']}")
        click.echo(f"Created Date: {result['created_date']}")
        click.echo(f"DB A Count: {result['db_a_count']}")
        click.echo(f"DB B Count: {result['db_b_count']}")
        click.echo(f"Discrepancy Count: {result['discrepancy_count']}")
        
        if result['notes']:
            click.echo(f"Notes: {result['notes']}")
        
        # Save full payload to file
        filename = f"full_payload_{do_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(result['payload_data'], f, indent=2)
        
        click.echo(f"\nFull payload saved to: {filename}")

@cli.command()
def interactive_mode():
    """Interactive mode for order cleaning"""
    click.echo("Welcome to Order Cleaning Interactive Mode!")
    click.echo("=" * 50)
    
    while True:
        click.echo("\nAvailable options:")
        click.echo("1. Compare data by date range")
        click.echo("2. Create payload for specific do_number")
        click.echo("3. List payload results")
        click.echo("4. Get specific payload")
        click.echo("5. Exit")
        
        choice = click.prompt("\nEnter your choice (1-5)", type=int)
        
        if choice == 1:
            start_date = click.prompt("Enter start date (YYYY-MM-DD)")
            end_date = click.prompt("Enter end date (YYYY-MM-DD)")
            
            data = {
                "start_date": start_date,
                "end_date": end_date
            }
            
            result = make_request("POST", "/compare-data", data)
            
            if result:
                click.echo(f"\n{result['message']}")
                click.echo(f"Total discrepancies: {result['total_discrepancies']}")
                
                if result['discrepancies']:
                    click.echo("\nDiscrepancies found:")
                    for i, disc in enumerate(result['discrepancies'], 1):
                        click.echo(f"{i}. {disc['do_number']} - DB A: {disc['db_a_count']}, DB B: {disc['db_b_count']}, Diff: {disc['discrepancy_count']}")
                    
                    # Ask if user wants to create payloads
                    create_choice = click.prompt("\nDo you want to create payloads? (y/n)", type=str, default="n")
                    
                    if create_choice.lower() == 'y':
                        for disc in result['discrepancies']:
                            do_number = disc['do_number']
                            click.echo(f"\nCreating payload for {do_number}...")
                            
                            payload_result = make_request("POST", f"/create-payload/{do_number}")
                            if payload_result:
                                click.echo(f"✓ Payload created for {do_number}")
                            else:
                                click.echo(f"✗ Failed to create payload for {do_number}")
        
        elif choice == 2:
            do_number = click.prompt("Enter do_number")
            create_payload(do_number)
        
        elif choice == 3:
            limit = click.prompt("Enter limit", type=int, default=100)
            offset = click.prompt("Enter offset", type=int, default=0)
            
            result = make_request("GET", f"/payload-results?limit={limit}&offset={offset}")
            
            if result:
                click.echo(f"\n{result['message']}")
                for payload in result['results']:
                    click.echo(f"- {payload['do_number']} ({payload['status']})")
        
        elif choice == 4:
            do_number = click.prompt("Enter do_number")
            get_payload(do_number)
        
        elif choice == 5:
            click.echo("Goodbye!")
            break
        
        else:
            click.echo("Invalid choice. Please try again.")

if __name__ == "__main__":
    cli() 