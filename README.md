# Immich Uploader

A Python script to upload photos and videos to a self-hosted Immich photo solution.

## Features

- **Batch Upload**: Upload photos and videos from local directories to Immich
- **Duplicate Detection**: Avoid re-uploading existing files using filename/size or SHA1 checksum
- **Album Creation**: Automatically create albums based on folder names
- **Multi-threading**: Concurrent uploads for faster processing
- **Recursive Upload**: Include subdirectories in uploads
- **Trash Sync**: Delete local files that have been marked as trash in Immich
- **Configuration Management**: Store API credentials securely in a config file
- **Test Mode**: Preview what files would be uploaded without actually uploading

## Requirements

- Python 3.x
- `requests` library: `pip3 install requests`
- `pyyaml` library: `pip3 install pyyaml`
- Access to an Immich server instance

## Installation

1. Clone or download this repository
2. Install required Python packages:
   ```bash
   pip3 install requests pyyaml
   ```
3. Run the configuration command to set up your Immich connection:
   ```bash
   python3 immic_uploader.py config
   ```

## Configuration

Run the configuration command to set up your connection to Immich:

```bash
python3 immic_uploader.py config
```

You'll be prompted to enter:
- **API Endpoint**: Your Immich server URL (e.g., `http://your-server:2283/api`)
- **API Key**: Your Immich API key (obtain from Immich web interface)
- **File Extensions**: Comma-separated list of file extensions to upload (default: `png,jpg,heic`)

Configuration is saved to `immic.config` in the current directory.

## Usage

### Basic Upload

Upload all supported files from a directory:

```bash
python3 immic_uploader.py /path/to/photos
```

### Upload with Options

```bash
# Upload recursively (including subdirectories)
python3 immic_uploader.py -r /path/to/photos

# Create albums based on folder names
python3 immic_uploader.py -a /path/to/photos

# Use SHA1 checksum for duplicate detection (slower but more accurate)
python3 immic_uploader.py -s /path/to/photos

# Test run (preview what would be uploaded)
python3 immic_uploader.py -t /path/to/photos

# Set maximum number of concurrent upload workers
python3 immic_uploader.py -m 5 /path/to/photos

# Delete local files that are marked as trash in Immich
python3 immic_uploader.py --deletelocal /path/to/photos

# Delete all albums created by this script
python3 immic_uploader.py --deletealbum
```

### Command Line Options

- `-a, --album`: Create albums based on directory names
- `-r, --recursive`: Include subdirectories in upload
- `-t, --test`: Test mode - show what would be uploaded without uploading
- `-m, --max_worker`: Maximum number of concurrent upload threads (default: 20)
- `-s, --sha1`: Use SHA1 checksum for duplicate detection
- `--deletelocal`: Remove local files that are trashed in Immich
- `--deletealbum`: Delete all albums created by this script

## Supported File Types

The script supports common image and video formats. Configure supported extensions in the config file:

- Images: `png`, `jpg`, `jpeg`, `heic`, `tiff`, `gif`, `bmp`
- Videos: `mp4`, `mov`, `avi`, `mkv`, `wmv`, `flv`

## How It Works

1. **Authentication**: Uses API key authentication with your Immich server
2. **Duplicate Check**: Compares files against existing assets using filename/size or SHA1 hash
3. **Upload**: Files are uploaded concurrently using multiple threads
4. **Album Sync**: Optionally creates albums and associates uploaded photos with them
5. **Cleanup**: Can remove local files that have been deleted in Immich

## Troubleshooting

- **Connection Errors**: Verify your API endpoint URL and ensure the Immich server is running
- **Authentication Issues**: Check your API key is correct and has upload permissions
- **Upload Failures**: Check file permissions and available disk space on the Immich server
- **Slow Uploads**: Adjust the `--max_worker` parameter or check network connectivity

## Compatibility

This script is compatible with Immich version 1.115+. It may not work with older versions due to API changes.

## License

This project is open source. Please check the license file for details.
