# pip install pytube

from pytube import Playlist

def download_playlist(url):
    try:
        playlist = Playlist(url)
        print(f"Playlist Title: {playlist.title}")
        
        # Iterate through each video in the playlist
        for video in playlist.videos:
            print(f"Downloading: {video.title}")
            # Get the highest resolution stream with both video and audio
            stream = video.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
            if stream:
                stream.download()  # Download the highest resolution stream with both video and audio
                print(f"Downloaded: {video.title}")
            else:
                print(f"No streams available for {video.title}")
        
        print("Download completed successfully!")
    except Exception as e:
        print(f"Error: {e}")

# Example usage:
playlist_url = "https://www.youtube.com/playlist?list=PLMWaZteqtEaLTJffbbBzVOv9C0otal1FO"
download_playlist(playlist_url)


# 1. https://www.youtube.com/playlist?list=PLMWaZteqtEaLTJffbbBzVOv9C0otal1FO - Wafa studies
# 2. https://www.youtube.com/playlist?list=PLNRxk1s77zfjX_3ktp5sKsOh4Q2cWMMDX - SS unitech
# 3. https://www.youtube.com/watch?v=ZgYotyUrnxs&list=PL7ZG6NdDdT8N8sfWViyEdReWoR_JjBSu_ - databag
# 4. https://www.youtube.com/playlist?list=PLsJW07-_K61KkcLWfb7D2sM3QrM8BiiYB - Annu Kumari
# 