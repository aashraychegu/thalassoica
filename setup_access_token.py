#!/usr/bin/env python3
import requests
import getpass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv, set_key
import os

ENV_FILE = '.env'
TOKEN_URL = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'


def get_valid_access_token(env_file: str = ENV_FILE) -> Optional[str]:
    """
    Get a valid access token, refreshing if necessary.
    
    Args:
        env_file: Path to .env file (default: '.env')
        
    Returns:
        Valid access token string or None if unable to obtain one
        
    Example:
        >>> token = get_valid_access_token()
        >>> if token:
        >>>     headers = {'Authorization': f'Bearer {token}'}
    """
    if not Path(env_file).exists():
        return None
    
    # Load environment variables
    load_dotenv(env_file, override=True)
    
    access_token = os.getenv('ACCESS_TOKEN')
    access_expires = os.getenv('ACCESS_TOKEN_EXPIRES_AT')
    refresh_token = os.getenv('REFRESH_TOKEN')
    
    # Check if access token is still valid (60 second buffer)
    expires_at = datetime.fromisoformat(access_expires)
    if (expires_at - datetime.now()).total_seconds() >= 60:
        return access_token
    
    # Refresh the access token
    response = requests.post(
        TOKEN_URL,
        data={
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': 'cdse-public'
        },
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    response.raise_for_status()
    token_data = response.json()
    
    # Save new token data
    _save_token_data(token_data, env_file)
    
    return token_data['access_token']


def authenticate(username: str, password: str, env_file: str = ENV_FILE) -> str:
    """
    Authenticate with username and password to get new tokens.
    
    Args:
        username: Copernicus username
        password: Copernicus password
        env_file: Path to .env file
        
    Returns:
        Access token string
    """
    response = requests.post(
        TOKEN_URL,
        data={
            'client_id': 'cdse-public',
            'username': username,
            'password': password,
            'grant_type': 'password'
        }
    )
    response.raise_for_status()
    token_data = response.json()
    
    # Save token data
    _save_token_data(token_data, env_file)
    
    print(f"✓ Authentication successful")
    return token_data['access_token']


def _save_token_data(token_data: dict, env_file: str):
    """Save token data to .env file."""
    now = datetime.now()
    access_expires_at = now + timedelta(seconds=token_data['expires_in'])
    refresh_expires_at = now + timedelta(seconds=token_data['refresh_expires_in'])
    
    Path(env_file).touch(exist_ok=True)
    
    set_key(env_file, 'ACCESS_TOKEN', token_data['access_token'])
    set_key(env_file, 'ACCESS_TOKEN_EXPIRES_AT', access_expires_at.isoformat())
    set_key(env_file, 'REFRESH_TOKEN', token_data['refresh_token'])
    set_key(env_file, 'REFRESH_TOKEN_EXPIRES_AT', refresh_expires_at.isoformat())
    set_key(env_file, 'TOKEN_TYPE', token_data.get('token_type', 'Bearer'))
    
    print(f"✓ Tokens saved to {env_file}")
    print(f"✓ Access token expires at: {access_expires_at.strftime('%Y-%m-%d %H:%M:%S')}")


def display_usage(env_file: str = ENV_FILE):
    """Display usage instructions."""
    print(f"\n{'='*60}")
    print("To use in Python:")
    print(f"{'='*60}")
    print(f"  from setup_access_token import get_valid_access_token")
    print(f"  ")
    print(f"  token = get_valid_access_token()")
    print(f"  headers = {{'Authorization': f'Bearer {{token}}'}}")
    print(f"\n{'='*60}")
    print("To use in bash:")
    print(f"{'='*60}")
    print(f"  source {env_file}")


def main():
    """CLI interface for token management."""
    print("=" * 60)
    print("Copernicus Data Space Ecosystem - Token Manager")
    print("=" * 60)
    
    # Try to get existing valid token
    existing_token = get_valid_access_token()
    
    if existing_token:
        load_dotenv(ENV_FILE)
        print("\n✓ Valid tokens found")
        print(f"  Access token expires at: {os.getenv('ACCESS_TOKEN_EXPIRES_AT')}")
        print(f"  Refresh token expires at: {os.getenv('REFRESH_TOKEN_EXPIRES_AT')}")
        
        choice = input("\nGet new tokens anyway? (y/n): ").strip().lower()
        if choice != 'y':
            display_usage()
            return
    
    # Authenticate
    print("\n" + "=" * 60)
    print("Authentication Required")
    print("=" * 60)
    
    username = input("Enter your username: ").strip()
    password = getpass.getpass("Enter your password: ")
    
    authenticate(username, password)
    display_usage()


if __name__ == "__main__":
    main()