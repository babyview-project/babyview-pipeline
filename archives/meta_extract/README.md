# Notes on meta data

## ACCL, GYRO, and GRAV

ACCL has gravity in it. GRAV is useful in telling what the gravity direction is. But the data order of ACCL and GRAV seem to be different. ACCL is claimed to be `Y,-X,Z` ([link](https://github.com/gopro/gpmf-parser#hero6-black-adds-and-changes-otherwise-supports-all-hero5-metadata)). If true, GRAV might be `-X,Y,Z`.

You need to first install [gpmf_parser](https://github.com/gopro/gpmf-parser.git) through cmake (`cmake .`) and make.

Then run command like:

```
./gpmf-parser input.mp4 -fACCL -a | tee accl_meta.txt
./gpmf-parser input.mp4 -fGYRO -a | tee gyro_meta.txt
./gpmf-parser input.mp4 -fGRAV -a | tee grav_meta.txt
```

All metas are:
```
ACCL
GYRO
SHUT
WBAL
WRGB
ISOE
UNIF
FACE
GPS5
CORI
IORI
GRAV
WNDM
MWET
AALP
MSKP
LSKP
```

## Highlights Flags

`python get_highlight_flags.py [video_file_name]`

It seems that the videos cannot be editted. 
Use python version bigger than 3.

## Device Serial Numbers
`python get_device_id.py [video_file_name]`

## Fisheye correction related

Using `ffmpeg` from [link](https://codex.so/fix-fisheye-distortion-for-gopro-videos).

Correction using checkboard stimulus at [link](https://www.theeminentcodfish.com/gopro-calibration/).

## Change to libx265 format

`ffmpeg -i input.mp4 -c:v libx265 -vtag hvc1 -strict -2 output.mp4`
