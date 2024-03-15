from dbricks import (
    Session, 
    EntityBase, 
    Column, 
    Integer, 
    String, 
    Float
)

class Rules(EntityBase):
    __tablename__ = "rules"
    __table_args__ = {
        "schema": "cme_fuzzy"
    }
    Rule_ID = Column(String, primary_key=True, nullable=False)
    Rule_Description = Column(String, nullable=False)
    Weighting_Order = Column(Integer, nullable=False)
    Weighting_Factor = Column(Float, nullable=False)
    
def main():
    print("Establishing session...")
    db = Session()
    print("Complete.")
    
    print("Retrieving rules...")
    rlz = db.query(Rules).all()
    print(f"Found {len(rlz)} rules:")
    for rl in rlz:
        print(f"- {rl.Rule_ID}")
        
    print("Execution complete.")
       
if __name__ == "__main__":
    main()