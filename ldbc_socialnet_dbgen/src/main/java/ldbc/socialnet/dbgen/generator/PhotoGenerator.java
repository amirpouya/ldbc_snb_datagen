/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.socialnet.dbgen.generator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.dictionary.PopularPlacesDictionary;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.PopularPlace;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


public class PhotoGenerator {
	
	DateGenerator		dateGenerator;
	Vector<Location>	vLocations;
	PopularPlacesDictionary dicPopularPlaces; 
	Random 				rand;
	Random				randLikes;
	int					maxNumUserTags;

	Random				randPopularPlaces;
	Random				randPopularPlacesId; 
	double				probPopularPlaces;			
	
	public PhotoGenerator(DateGenerator _dateGen, Vector<Location> _vLocations, 
						long _seed, int _maxNumUserTags, PopularPlacesDictionary _dicPopularPlaces,
						double _probPopularPlaces){
		this.dateGenerator = _dateGen; 
		this.vLocations = _vLocations; 
		rand = new Random(_seed);
		randLikes = new Random(_seed);
		this.maxNumUserTags = _maxNumUserTags; 
		this.dicPopularPlaces = _dicPopularPlaces; 
		this.randPopularPlaces = new Random(_seed);
		this.randPopularPlacesId = new Random(_seed);
		this.probPopularPlaces = _probPopularPlaces;
	}
	
	public Photo generatePhoto(ReducedUserProfile user, Group album, 
								int idxInAlbum, int maxNumLikes){
		ScalableGenerator.postId++;
		Photo photo = new Photo();
		
		photo.setAlbumId(album.getForumWallId());
		photo.setCreatorId(album.getModeratorId());
		int locationIdx = album.getLocationIdx();
		byte numPopularPlace = user.getNumPopularPlace();
		
		photo.setLocationIdx(locationIdx);
		Location location = vLocations.get(locationIdx);
		if (numPopularPlace == 0){
			photo.setLocationName(location.getName());
			photo.setLatt(location.getLatt());
			photo.setLongt(location.getLongt());
		}
		else{
			int popularPlaceId;
			PopularPlace popularPlace;
			if (randPopularPlaces.nextDouble() < probPopularPlaces){
				//Generate photo information from user's popular place
				int popularIndex = randPopularPlacesId.nextInt(numPopularPlace);
				popularPlaceId = user.getPopularId(popularIndex);
				popularPlace = dicPopularPlaces.getPopularPlace(user.getLocationIdx(),popularPlaceId);
				photo.setLocationName(popularPlace.getName());
				photo.setLatt(popularPlace.getLatt());
				photo.setLongt(popularPlace.getLongt());
			}
			
			else{
				// Randomly select one places from Album location idx
				popularPlaceId = dicPopularPlaces.getPopularPlace(locationIdx);
				if (popularPlaceId != -1){
					popularPlace = dicPopularPlaces.getPopularPlace(locationIdx,popularPlaceId);
					photo.setLocationName(popularPlace.getName());
					//System.out.println("Set popular place name " + photo.getLocationName());
					photo.setLatt(popularPlace.getLatt());
					photo.setLongt(popularPlace.getLongt());
				}
				else{
					photo.setLocationName(location.getName());
					photo.setLatt(location.getLatt());
					photo.setLongt(location.getLongt());
				}
			}
		}
		
		photo.setPhotoId(ScalableGenerator.postId);
		photo.setImage("photo" + photo.getPhotoId() + ".jpg");
		
		//Assume that the photo are created one by one after 1 second from the creation of the album
		photo.setTakenTime(album.getCreatedDate() + 1000*(idxInAlbum+1));	
		
		HashSet<Integer> tags = new HashSet<Integer>();
        Iterator<Integer> it = user.getSetOfTags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (tags.isEmpty()) {
                tags.add(value);
            } else {
                if (rand.nextDouble() < 0.2) {
                    tags.add(value);
                }
            }
        }
        photo.setTags(tags);
		
		int numberOfLikes = randLikes.nextInt(maxNumLikes);
		
		int[] likes = getFriendsLiked(album, numberOfLikes);
		photo.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.sevenDayInMillis+photo.getTakenTime());
        }
        photo.setInterestedUserAccsTimestamp(likeTimestamp);
		
		return photo; 
	}
	
	public int[] getFriendsLiked(Group album, int numOfLikes){
		GroupMemberShip fullMembers[] = album.getMemberShips();
		
		int friends[];
		if (numOfLikes >= album.getNumMemberAdded()){
			friends = new int[album.getNumMemberAdded()];
			for (int j = 0; j < album.getNumMemberAdded(); j++){
				friends[j] = fullMembers[j].getUserId();
			}
		} else{
			friends = new int[numOfLikes];
			int startIdx = randLikes.nextInt(album.getNumMemberAdded() - numOfLikes);
			for (int j = 0; j < numOfLikes; j++){
				friends[j] = fullMembers[j+startIdx].getUserId();
			}			
		}
		
		return friends; 
		 
	}
	
	public Vector<Location> getvLocations() {
		return vLocations;
	}
	public void setvLocations(Vector<Location> vLocations) {
		this.vLocations = vLocations;
	}
	
}
