package api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;

@Data
public class Movie {
  private String cast;
  private String country;
  @JsonProperty("show_id")
  private String showId;
  private String director;
  @JsonProperty("release_year")
  private Long releaseYear;
  private String rating;
  private String description;
  private String type;
  private String title;
  @JsonSerialize
  private String duration;
  @JsonProperty("listed_in")
  private String listedIn;
  @JsonProperty("date_added")
  private String dateAdded;

  @Override
  public String toString() {
    return "Movie{" +
            "country='" + country + '\'' +
            ", showId='" + showId + '\'' +
            ", director='" + director + '\'' +
            ", releaseYear=" + releaseYear +
            ", rating='" + rating + '\'' +
            ", description='" + description + '\'' +
            ", type='" + type + '\'' +
            ", title='" + title + '\'' +
            ", duration='" + duration + '\'' +
            ", listedIn='" + listedIn + '\'' +
            ", dateAdded='" + dateAdded + '\'' +
            '}';
  }
}