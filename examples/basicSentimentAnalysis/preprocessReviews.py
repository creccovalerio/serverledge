import re
import json

def handler(params, context):
    return preprocessReviews(params["input"])

def preprocessReviews(reviews):
    """
    Preprocesses reviews provided as a JSON-encoded string.
    - Converts the JSON string to a Python list
    - Cleans each review by removing punctuation, converting to lowercase, and removing stopwords
    """
    # Decode the JSON string into a Python list
    reviews = json.loads(reviews)
    
    # A minimal set of common English stopwords
    stop_words = {
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", 
        "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", 
        "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", 
        "theirs", "themselves", "what", "which", "who", "whom", "this", "that", 
        "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", 
        "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", 
        "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", 
        "at", "by", "for", "with", "about", "against", "between", "into", "through", 
        "during", "before", "after", "above", "below", "to", "from", "up", "down", 
        "in", "out", "on", "off", "over", "under", "again", "further", "then", 
        "once", "here", "there", "when", "where", "why", "how", "all", "any", 
        "both", "each", "few", "more", "most", "other", "some", "such", "no", 
        "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", 
        "t", "can", "will", "just", "don", "should", "now"
    }
    
    preprocessed_reviews = []
    for review in reviews:
        review = review.lower()  # Convert to lowercase
        review = re.sub(r'[^\w\s]', '', review)  # Remove punctuation
        words = review.split()  # Split into words
        filtered_words = [word for word in words if word not in stop_words]  # Remove stopwords
        preprocessed_review = ' '.join(filtered_words)  # Join words back into a single string
        preprocessed_reviews.append(preprocessed_review)
    
    return preprocessed_reviews
