# Fruita LiDAR Data

## Source

Mesa County, Colorado QL2 LiDAR

Downloaded:
2026-05-18

Original products:

- 290235.las
- 290235.copc.laz
- 290235.img

Metadata:

- FolderID_54_utm13_Mesa_County_CO_QL2_Lidar_DEM_IMG.xml
- FolderID_54_utm13_Mesa_County_CO_QL2_Lidar_LAS_1.4.xml

Coordinate System

Horizontal:
NAD83(HARN) UTM Zone 12N

Vertical:
US Survey Feet

Point Cloud

LAS version 1.4

Ground points:
Classification = 2

Point count:
10,284,201

Ground point count:
7,494,813

## Derived Products

Fruita_Field_DEM_2016_2ft_from_las.tif

Generated with:

PDAL 2.10.2

Input:
290235.las

Ground filter:
Classification == 2

Grid spacing:
2 ft

Output type:
minimum elevation

NoData:
-9999

## Clipped Products

Fruita_Field_DEM_2016_2ft_clip_m.tif

Projection:
EPSG:3742

Resolution:
0.6096012192 m
(2.0 US Survey Feet)

Clip polygon:
field_boundary

## Initial Findings

2016 LiDAR indicates:

• Field relief:
    3.13 ft

Logger-row elevation ranges

Top:
0.45 ft

Middle:
0.89 ft

Bottom:
1.01 ft

A pronounced north-south linear depression is visible
west of Strip 1.

Origin currently unknown.