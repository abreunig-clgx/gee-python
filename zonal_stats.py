
def bigqee_zonalstats(
        service_account,
        service_account_key_file,
        BigQuery_projectID,
        BigQuery_SQL,
        image_collection,
        start_date,
        end_date,
        outputTable,
        outputProjectID,
        stats=['mean', 'median', 'max', 'min', 'stdDev', 'sum'],
        band_calc={'NDVI': ['B8', 'B4']},
        scale=10
        
):
        """
        This function uses Google Earth Engine to calculate zonal statistics for a
        given BigQuery SQL query. The function returns a GeoJSON object with the
        zonal statistics.
        
        Parameters
        ----------
        service_account : str
                The service account name to be used for authentication.
        service_account_key_file : str
                The path to the service account key file.
        BigQuery_projectID : str
                The BigQuery project ID.
        BigQuery_SQL : str
                The BigQuery SQL query.
        image_collection : str
                The Earth Engine image collection URL to be used for zonal statistics.
        start_date : str
                The start date for the Earth Engine image collection (inclusive).
        end_date : str
                The end date for the Earth Engine image collection (Exclusive).

        stats : list
                The list of zonal statistics to be calculated. The options are:
                ['mean', 'median', 'max', 'min', 'stdDev', 'sum'].

        band_calc : dict
                The dictionary possible band calcualtions. The default is
                NDVI: 'NDVI = (NIR - RED) / (NIR + RED)'. {'NDVI': [NIR_Band, RED_Band]}

        Returns
        -------
        geojson : object
                The GeoJSON object with the zonal statistics.
        """
        import ee
        import geojson
        import pandas_gbq as gbq
        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import shape
        from shapely import wkt
        from collections import defaultdict
        
        # Authenticate to Google Cloud
        credentials = ee.ServiceAccountCredentials(
                service_account,
                service_account_key_file
        )
        ee.Initialize(credentials)
        
        # Get the BigQuery table
        table = gbq.read_gbq(
                BigQuery_SQL,
                project_id=BigQuery_projectID,
                dialect='standard'
        )
        
        # if table is empty, throw error
        if table.empty:
                raise ValueError('The BigQuery table is empty.')
        
        # detect the geometry column
        geometry_column = None
        for column in table.columns:
                if column.lower() == 'geometry' or column.lower() == 'geography' or column.lower() == 'geom':
                        geometry_column = column
                        break
        if geometry_column is None:
                raise ValueError('The BigQuery table does not have a geometry column.')
        
        # convert the geometry column to GeoJSON
        table[geometry_column] = table[geometry_column].apply(lambda x: geojson.Feature(geometry=wkt.loads(x)))

        # loop through the stats and calculate zonal stats on each feature in table
        # initialize the image collection
        image_collection = ee.ImageCollection(image_collection)\
                .filterDate(start_date, end_date)

        # check if band_calc is empty
        if band_calc:
        # conduct NDVI calculation
                bands = [val for val in band_calc.values()][0]
                

        zonal_df = pd.DataFrame()
        for stat in stats:
                print(stat)
                options = defaultdict(lambda: ee.Reducer.sum(),
                                {
                                        'mean': ee.Reducer.mean(), 
                                        'median': ee.Reducer.median(),
                                        'max': ee.Reducer.max(),
                                        'min': ee.Reducer.min(),
                                        'stdDev': ee.Reducer.stdDev()
                                }
                )
                # get the reducer
                reducer = options[stat]
                
                  # Convert the result to an Earth Engine feature

                # Convert the Pandas DataFrame to an Earth Engine FeatureCollection
                #rint(table['geom']['geometry'])
                #table.to_csv('table.csv')
                print('bigq to ee feature colleciton')
                ee_table = ee.FeatureCollection(table['geom'].to_list())
                print('image_collection filter bounds')
                aoi_col = image_collection.filterBounds(ee_table)
                print('calculate ndvi')
                ndvi_col = aoi_col.map(lambda image: image.normalizedDifference(bands))

                print('Calculate zonal statistics...')
                                # Define a function to calculate zonal statistics for a single feature
                def calculate_zonal_stats(feature):
                        # Extract the geometry from the feature
                        zone = feature.geometry()

                        # Filter the image collection to get an image intersecting the zone
                        ndvi = ndvi_col.first()
                        
                        # Use server-side condition to check if ndvi is empty
                        is_empty = ee.Algorithms.IsEqual(ndvi, None)
                        
                        # Use ee.Algorithms.If() to handle the condition
                        zonal_stats = ee.Algorithms.If(is_empty, ee.Dictionary({'empty': 1}), ndvi.reduceRegion(
                                reducer=reducer,
                                geometry=zone,
                                scale=scale
                        ))

                        # Return a feature with the zonal statistics as properties
                        return feature.set(zonal_stats)

                
                # Use map() to apply the function to the FeatureCollection
                zonal_stats_collection = ee_table.map(calculate_zonal_stats)
                zonal_df = pd.DataFrame(zonal_stats_collection.getInfo()['features'])

        
                # back to wkt
                zonal_df['geometry'] = zonal_df['geometry'].apply(lambda x: shape(x).wkt)

                # extract NDVi values
                zonal_df['NDVI'] = zonal_df['properties'].apply(lambda x: x['nd'] if x else None)
                zonal_df = zonal_df[['NDVI', 'geometry']]
                

                #return zonal_df
                gbq.to_gbq(zonal_df, outputTable, project_id=outputProjectID, if_exists='replace')
        
        
if 'Name' == '__main__':
        # test the function
        service_account = input('Enter the service account name: ')
        service_account_key_file = input('Enter the service account key file path: ')
        BigQuery_projectID = input('Enter the BigQuery project ID: ')
        BigQuery_SQL = input('Enter the BigQuery SQL query: ')
        image_collection = input('Enter the Earth Engine image collection URL: ')
        start_date = input('Enter the start date for the image collection (inclusive): ')
        end_date = input('Enter the end date for the image collection (exclusive): ')
        stats = input('Enter the zonal statistics to be calculated: eg. mean, median, max, min, stdDev, sum: ')
        band_calc = input('Enter the band calculation: eg. NDVI: B8, B4: ')
        scale = input('Enter the scale: i.e. match imagery spatial resolution')
        outputTable = input('Enter the output table name: ')
        outputProjectID = input('Enter the output project ID: ')


