#!/usr/bin/env python

"""
reset_checkpoints.py

Reset Kafka checkpoints to force stream replay from a specific offset.

Usage:
    python reset_checkpoints.py --consumer customers --offset earliest
    python reset_checkpoints.py --consumer all --offset earliest
    python reset_checkpoints.py --consumer products --offset 5000
"""

import argparse
import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config
from reload_utils import reset_checkpoint, get_checkpoint_info

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_CONSUMERS = ["customers", "products", "orders", "order_items"]


def main():
    parser = argparse.ArgumentParser(
        description="Reset Kafka checkpoints to force replay"
    )
    parser.add_argument(
        "--consumer",
        required=True,
        help=f"Consumer to reset. Options: {', '.join(VALID_CONSUMERS)}, or 'all'"
    )
    parser.add_argument(
        "--offset",
        required=True,
        help="Target offset: 'earliest', 'latest', or a specific offset number (e.g., 5000)"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt (use with caution)"
    )
    
    args = parser.parse_args()
    
    # Validate consumer input
    if args.consumer.lower() == "all":
        consumers = VALID_CONSUMERS
    elif args.consumer.lower() in VALID_CONSUMERS:
        consumers = [args.consumer.lower()]
    else:
        logger.error(f"Unknown consumer: {args.consumer}")
        logger.error(f"Valid options: {', '.join(VALID_CONSUMERS)}, all")
        sys.exit(1)
    
    # Validate offset
    if args.offset.lower() not in ["earliest", "latest"]:
        try:
            int(args.offset)
        except ValueError:
            logger.error(f"Invalid offset: {args.offset}. Use 'earliest', 'latest', or a number")
            sys.exit(1)
    
    # Confirmation
    if not args.confirm:
        print(f"\n⚠️  WARNING: This will reset checkpoints for: {', '.join(consumers)}")
        print(f"   Streams will replay from offset: {args.offset}")
        print("\n   This can take a long time if there's a large backlog!")
        
        response = input("\nAre you sure? Type 'YES' to confirm: ")
        if response.strip() != "YES":
            logger.info("Reset cancelled.")
            sys.exit(0)
    
    try:
        checkpoint_base = config.get("checkpoint_base")
        if not checkpoint_base:
            logger.error("checkpoint_base not found in config")
            sys.exit(1)
        
        logger.info(f"\n=== CHECKPOINT STATUS BEFORE RESET ===")
        for consumer in consumers:
            checkpoint_path = os.path.join(checkpoint_base, consumer)
            info = get_checkpoint_info(checkpoint_path)
            logger.info(f"{consumer}: {info}")
        
        logger.info(f"\n=== RESETTING CHECKPOINTS ===")
        
        for consumer in consumers:
            checkpoint_path = os.path.join(checkpoint_base, consumer)
            logger.info(f"\nResetting: {consumer}")
            logger.info(f"  Path: {checkpoint_path}")
            logger.info(f"  Target offset: {args.offset}")
            
            try:
                reset_checkpoint(checkpoint_path)
                logger.info(f"  ✓ Reset successful")
            except Exception as e:
                logger.error(f"  ✗ Reset failed: {e}")
        
        logger.info(f"\n=== CHECKPOINT STATUS AFTER RESET ===")
        for consumer in consumers:
            checkpoint_path = os.path.join(checkpoint_base, consumer)
            info = get_checkpoint_info(checkpoint_path)
            logger.info(f"{consumer}: {info}")
        
        logger.info(f"\n✓ Checkpoint reset complete")
        logger.info(f"  Next stream restart will begin from offset: {args.offset}")
        logger.info(f"  Monitor logs for replay progress (may take a while)")
        
    except Exception as e:
        logger.error(f"Checkpoint reset failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
