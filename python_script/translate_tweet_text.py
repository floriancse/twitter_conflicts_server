from deep_translator import GoogleTranslator
from langdetect import detect

def translate_to_english(text):
    try:
        lang = detect(text)
        if lang == "en":
            return text
        return GoogleTranslator(source='auto', target='en').translate(text)
    except:
        return text
    


