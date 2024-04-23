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
playlist_url = "https://www.youtube.com/watch?v=JIJEL7M7Pv0&list=PLWf6TEjiiuICyhzYAnSshwQQy3hrH3eGw"
download_playlist(playlist_url)


# 01. https://www.youtube.com/playlist?list=PLMWaZteqtEaLTJffbbBzVOv9C0otal1FO -- WafaStudies - DONE
# 02. https://www.youtube.com/playlist?list=PLMWaZteqtEaLacN3eS3s8pw2jtwBVb1BH -- WafaStudies - DONE
# 03. https://www.youtube.com/watch?v=JIJEL7M7Pv0&list=PLWf6TEjiiuICyhzYAnSshwQQy3hrH3eGw -- TechBrothersIT -- In Progress
# 04. https://www.youtube.com/playlist?list=PLNRxk1s77zfjX_3ktp5sKsOh4Q2cWMMDX -- SS unitech -- DONE
# 05. https://www.youtube.com/playlist?list=PLsJW07-_K61KkcLWfb7D2sM3QrM8BiiYB - Annu Kumari
# 06. https://www.youtube.com/watch?v=P4uPw7FNnp0&list=PLm7Nm9btqfe3QNbrLY-Vyu8-wh3EanG6t -- All Abou IT -- DONE
# 07. https://www.youtube.com/watch?v=ZgYotyUrnxs&list=PL7ZG6NdDdT8N8sfWViyEdReWoR_JjBSu_ -- Databag -- DONE