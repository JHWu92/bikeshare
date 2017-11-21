# coding=utf-8
import geopandas as gp
from shapely.geometry import Point, LineString, Polygon


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    from math import radians, cos, sin, asin, sqrt
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    m = km * 1000
    return m


def distance_diff(locs):
    return [haversine(loni, lati, loni1, lati1) for (loni, lati), (loni1, lati1) in zip(locs, locs[1:])]


def ptfromln(pt, ln):
    """
    project pt to ln, compute haversine distance between projected pt and pt
    :param pt: shapely.geometry.Point, (lon, lat) point
    :param ln: shapely.geometry.LineString, [(lon,lat)] points
    :return: distance in meters
    """
    n_pt = ln.interpolate(ln.project(pt))
    lon1, lat1 = n_pt.coords[0]
    lon2, lat2 = pt.coords[0]
    return haversine(lon1, lat1, lon2, lat2)

def crs_prepossess(gpdf, init_crs, bfr_crs):
    """
    create a shallow copy of gpdf; check the init crs of gpdf, if None, assign init_crs; change crs of copy to bfr_crs
    :param gpdf: geopandas.GeoDataFrame
    :param init_crs: init_crs epsg code
    :param bfr_crs: target crs epsg code used for buffering
    :return: a shallow copy of gpdf in bfr_crs
    """
    gpdf_crs = gpdf.copy()
    if gpdf_crs.crs == None:
        gpdf_crs.crs = {'init': u'epsg:{}'.format(init_crs)}
    return gpdf_crs.to_crs(epsg=bfr_crs)


# ########## functions assigning ln(segment) to objs #############
def pts2segs(pts, lns, bfr_crs, init_crs=4326, close_jn_dist=5, far_jn_dist=20):
    """
    1. close jn: buffer pts in bfr_crs with close_jn_dist, use sjoin to find segment(s) intersected with buffered pts
    2. far jn: for pts without any segment in close jn, buffer them with far_jn_dist and find nearest segment
    :param pts: geopandas.GeoDataFrame
    :param lns: geopandas.GeoDataFrame
    :param bfr_crs: target crs epsg code used for buffering
    :param init_crs: init_crs epsg code, default 4326(lat lon)
    :param close_jn_dist: close join distance, allowing multiple segments for one point(assumed as intersection)
    :param far_jn_dist: far join distance, find the nearest segment for one point
    :return: pandas.DataFrame, columns=[pt_index, ln_index]
    """

    import pandas as pd
    index_pt, index_ln = 'index_pt', 'index_ln'

    lns_crs = crs_prepossess(lns, init_crs, bfr_crs)
    pts_crs = crs_prepossess(pts, init_crs, bfr_crs)

    close_jn = pts_crs.copy()
    close_jn.geometry = close_jn.buffer(close_jn_dist)
    try:
        close_jn = gp.tools.sjoin(close_jn, lns_crs)[['index_right']]
    except ValueError:  # no segment is matched during close join
        close_jn = pd.DataFrame([], columns=['index_right'])

    close_jn_pts = set(pd.unique(close_jn.index))
    far_jn = pts_crs[~pts_crs.index.isin(close_jn_pts)].copy()
    
    if not far_jn.empty:
        far_jn.geometry = far_jn.buffer(far_jn_dist)
        try:
            far_jn = gp.tools.sjoin(far_jn, lns_crs)[['index_right']]
            # calculate haversine distance
            far_jn = pd.merge(lns[['geometry']], far_jn, left_index=True, right_on=['index_right'])
            far_jn = pd.merge(pts[['geometry']], far_jn, left_index=True, right_index=True)
            far_jn['dis'] = far_jn.apply(lambda x: ptfromln(x.geometry_x, x.geometry_y), axis=1)
            # keep ln with minimum distance to pt
            far_jn = far_jn.groupby(level=0).apply(lambda x: x.iloc[x.dis.values.argmin()][['index_right']])
        except ValueError:  # no segment is matched during far join
            far_jn = pd.DataFrame([], columns=['index_right'])
    else:
        far_jn = pd.DataFrame([], columns=['index_right'])

    pts_has_ln = close_jn.append(far_jn).reset_index()
    pts_has_ln.columns = [index_pt, index_ln]
    pts_has_ln = pts_has_ln.astype(int)
    pts_no_ln = pts[~pts.index.isin(pd.unique(pts_has_ln[index_pt]))].copy()

    return pts_has_ln, pts_no_ln

