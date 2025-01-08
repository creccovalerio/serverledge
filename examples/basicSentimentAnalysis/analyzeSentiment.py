import json

def handler(params, context):
    return analyzeSentiment(params["input"])

def analyzeSentiment(reviews_json):
    """
    Analyzes sentiment for a JSON string representing tokenized reviews.
    - Counts the number of positive, negative, and neutral reviews.
    - Returns an array [positive_count, negative_count, neutral_count].
    """
    # Parse the JSON string to a Python object (list of lists)
    reviews = json.loads(reviews_json)
    
    # Define sets of positive and negative words
    positive_words = {
        "amazing", "great", "excellent", "good", "positive", 
        "happy", "love", "recommended", "wonderful", "satisfied",
        "fantastic", "perfect", "impressive", "outstanding", "superb", 
        "brilliant", "reliable", "exceptional", "thrilled", "awesome", 
        "delighted", "top-notch", "phenomenal", "flawless", "incredible", 
        "five", "stars", "highly"
    }

    negative_words = {
        "bad", "poor", "terrible", "awful", "negative", "hate", 
        "disappointed", "worse", "problematic", "unsatisfied",
        "horrible", "useless", "waste", "broken", "unreliable", 
        "flawed", "dissatisfied", "unacceptable", "dislike", "wrong", 
        "issues", "mediocre", "worthless", "unhappy", "avoid", 
        "low", "false", "misleading", "problem", "annoying"
    }
    
    # Initialize counters
    positive_count = 0
    
    for review_tokens in reviews:
        # Count positive and negative words in the review
        pos_count = sum(1 for word in review_tokens if word in positive_words)
        neg_count = sum(1 for word in review_tokens if word in negative_words)
        
        # Classify the review based on counts
        if pos_count > neg_count:
            positive_count += 1
    
    positive_percentage = positive_count / len(reviews)
    return positive_percentage