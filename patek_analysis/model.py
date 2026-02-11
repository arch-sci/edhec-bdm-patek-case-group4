
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from patek_analysis.data import get_patek_data

def train_price_predictor():
    # 1. Load Data
    print("⏳ Loading data for training...")
    df = get_patek_data()
    
    # 2. Select Target & Features
    target = 'price'
    # These are the columns we can use to determine value
    features = ['collection', 'reference_code', 'currency', 'life_span']
    
    # 3. Simple Cleanup
    # Remove rows where price is missing/empty
    df = df.dropna(subset=[target])
    
    # 4. Preprocessing: Convert Text columns to Numbers
    le = LabelEncoder()
    print("🔧 Preprocessing features...")
    
    for col in features:
        # Fill empty cells with 'Unknown' to prevent crashes
        df[col] = df[col].fillna('Unknown')
        # Force conversion to string and encode
        df[col] = le.fit_transform(df[col].astype(str))
        
    X = df[features]
    y = df[target]
    
    # 5. Split Data (80% used to learn, 20% used to test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 6. Train Model
    print("🤖 Training Random Forest Model...")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # 7. Evaluate
    score = model.score(X_test, y_test)
    print(f"🚀 Model Trained! R2 Score: {score:.2f} (1.0 is perfect)")
    
    return model

if __name__ == '__main__':
    train_price_predictor()