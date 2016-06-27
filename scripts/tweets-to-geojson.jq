{"type": "FeatureCollection",
                        "features": [.[] | (select(.geo != null) | {"type": "Feature",
                                                              "geometry": {"type": "Point",
                                                              "coordinates": [.geo.coordinates[1], .geo.coordinates[0]]},
                                                              "properties": {"user" : .user.screen_name,
                                                                             "text" : .text}})]}
