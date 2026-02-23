from yt_dlp import YoutubeDL

url = "https://x.com/i/status/2025827948666720288"
ydl_opts = {
    'quiet': True,              
    'no_warnings': True,
    'simulate': True,            
    'getthumbnail': True,       
}

with YoutubeDL(ydl_opts) as ydl:
    try:
        info = ydl.extract_info(url, download=False)
        thumbnail_url = info.get('thumbnail')
        
        if thumbnail_url:
            print(thumbnail_url)
            
    except Exception as e:
        print("Erreur :", e)