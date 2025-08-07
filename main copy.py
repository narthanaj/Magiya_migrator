#!/usr/bin/env python3

import sys
from colorama import init, Fore, Style  # Added Style import
from config import Config
from migration import MagiyaMigration
from validator import MigrationValidator
from rollback import MigrationRollback

init(autoreset=True)

def print_banner():
    """Print application banner"""
    print(f"""
{Fore.CYAN}╔══════════════════════════════════════╗
║     Magiya Database Migration Tool    ║
║           V1 → V2 Migration           ║
╚══════════════════════════════════════╝{Style.RESET_ALL}
    """)

def print_menu():
    """Print main menu"""
    print(f"\n{Fore.CYAN}Select an option:")
    print("1. Run full migration")
    print("2. Validate existing migration")
    print("3. Rollback migration")
    print("4. Exit")
    return input("\nEnter your choice (1-4): ")

def main():
    """Main application entry point"""
    print_banner()
    
    # Load configuration
    config = Config()
    
    while True:
        choice = print_menu()
        
        if choice == '1':
            # Run migration
            migration = MagiyaMigration(config)
            migration.run()
            
        elif choice == '2':
            # Validate migration
            validator = MigrationValidator(config)
            validator.validate()
            
        elif choice == '3':
            # Rollback migration
            rollback = MigrationRollback(config)
            rollback.rollback()
            
        elif choice == '4':
            print(f"\n{Fore.YELLOW}Goodbye!")
            sys.exit(0)
            
        else:
            print(f"{Fore.RED}Invalid choice. Please try again.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Fore.YELLOW}Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Fore.RED}Unexpected error: {e}")
        sys.exit(1)