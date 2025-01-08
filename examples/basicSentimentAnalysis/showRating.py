def handler(params, context):
    return showRating(float(params["input"]))

def showRating(rate):
    
    if rate >= 0.5 and rate < 0.75:
        return "This product has a GOOD positive ranting of: " + str(rate*100) +"%"
    elif rate >= 0.75 and rate <= 1.0: 
        return "This product has a HIGH positive ranting of: " + str(rate*100) +"%"      
    else:
        return "This product has a BAD positive ranting of: " + str(rate*100) +"%"