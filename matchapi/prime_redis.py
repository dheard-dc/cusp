import redis 
import logging
from brix import Brix

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    b = Brix()
    logger.info("Getting rules from Databricks...")
    recs = b.execute_query("SELECT * FROM cme_fuzzy.rules")
    logger.info("Found the following rules:")
    for rec in recs:
        logger.info(f"- {rec['Rule_ID']}")
    
def stringify_dict(data: dict):
    out = { k: str(v) for k, v in data.items() }
    return out
    
    
def cache_customers(src_id:str, table_name: str, max_records: int=10):
    b = Brix()
    
    logger.info("Establishing redis connection...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    logger.info("Connected.")
    
    logger.info(f"Deleting pre-existing keys for {src_id} source...")
    deleted = 0
    for key in r.scan_iter(f"cust:{src_id}:*"):
        deleted += 1
        r.delete(key)                           
    logger.info(f"{deleted} keys were deleted for {src_id} source.")
    
    logger.info(f"Retrieving customer data from {table_name} into redis cache...")
    sql = f"select * from {table_name} limit {max_records}"
    data = b.execute_query(sql)
    
    logger.info("Commencing cache of records to redis...")
    cached_recs = 0
    for rec in data:
        rec_id = rec['synthetic_key']
        rec_key = f"cust:{src_id}:{rec_id}"
        rec_data = rec
        
        # Save the data to 
        cached_recs += 1
        r.hset(
            rec_key,
            mapping=stringify_dict(rec_data)
        )
        
    logger.info(f"{cached_recs} Records have been cached to redis.")
        
def prime_raw():
    # Load the seed objects 
    cache_customers("dental", "cme_synthesis.dental_seed", 10)
    cache_customers("optical", "cme_synthesis.optical_seed", 10)
    cache_customers("member", "cme_synthesis.master_syn", 10)
    
def prime_map():
    b = Brix()
    
    logger.info("Establishing redis connection...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    logger.info("Connected.")
    
    logger.info("Retrieving match map...")
    sql = "SELECT * FROM cme_synthesis.match_map"
    data = b.execute_query(sql)
    
    logger.info("Commencing cache of records to redis...")
    cached_recs = 0
    for rec in data:
        rec_id = rec['synthetic_key']
        rec_key = f"map:{src_id}:{rec_id}"
        rec_data = rec
        
        # Save the data to 
        cached_recs += 1
        r.hset(
            rec_key,
            mapping=stringify_dict(rec_data)
        )
            