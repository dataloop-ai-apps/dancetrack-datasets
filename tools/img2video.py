from moviepy.editor import ImageSequenceClip
import os
import dtlpy as dl


def download_dataset(dataset: dl.Dataset, download_path: str):
    filters = dl.Filters()
    filters.add(field=dl.FiltersKnownFields.FILENAME, values="*.txt")
    dataset.items.download(local_path=download_path)


def images_to_video(image_folder, output_path, fps=20):
    # Load images from the folder and sort them by name to ensure correct sequence
    image_files = [f"{image_folder}/{img}" for img in sorted(os.listdir(image_folder)) if img.endswith(".jpg")]

    # Create a video clip from the images
    clip = ImageSequenceClip(image_files, fps=fps)

    # Write the output video file in WebM format
    clip.write_videofile(output_path, codec="libvpx-vp9")
