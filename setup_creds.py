#!/usr/bin/env python3
"""Interactive script to set up Telegram credentials in the database."""

import sys
from database import Database, DatabaseMigration


def main():
    print("TGFeed - Telegram Credentials Setup")
    print("=" * 40)
    print()

    # Run migrations to ensure tg_creds table exists
    DatabaseMigration().migrate()

    with Database() as db:
        # Show existing credentials
        creds = db.get_all_tg_creds()
        if creds:
            print("Existing credentials:")
            for c in creds:
                primary = " (primary)" if c.get("primary") else ""
                print(f"  [{c['id']}] {c['phone_number']}{primary}")
            print()

            choice = input("Add new credentials? (y/n): ").strip().lower()
            if choice != 'y':
                print("Exiting.")
                return
            print()

        # Get credentials from user
        print("Get your API credentials from https://my.telegram.org/apps")
        print()

        try:
            api_id = input("API ID: ").strip()
            if not api_id:
                print("Error: API ID is required")
                sys.exit(1)
            api_id = int(api_id)
        except ValueError:
            print("Error: API ID must be a number")
            sys.exit(1)

        api_hash = input("API Hash: ").strip()
        if not api_hash:
            print("Error: API Hash is required")
            sys.exit(1)

        phone_number = input("Phone number (with country code, e.g. +1234567890): ").strip()
        if not phone_number:
            print("Error: Phone number is required")
            sys.exit(1)
        if not phone_number.startswith('+'):
            phone_number = '+' + phone_number

        # Check if this should be the primary account
        existing = db.get_all_tg_creds()
        if existing:
            make_primary = input("Make this the primary account? (y/n): ").strip().lower() == 'y'
        else:
            make_primary = True

        # Add credentials
        cred_id = db.add_tg_creds(api_id, api_hash, phone_number, primary=make_primary)

        print()
        print(f"Credentials added successfully (ID: {cred_id})")
        if make_primary:
            print("This account is set as primary.")
        print()
        print("You can now start the daemon with: daemon.bat (Windows) or ./daemon.sh (Linux/macOS)")


if __name__ == "__main__":
    main()
