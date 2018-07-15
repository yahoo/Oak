package oak;

/**
 * Created by gsheffi on 26/06/2018.
 */
public interface SizeCalculator<T> {

  // returns the number of bytes needed for serializing the given object
  int calculateSize(T object);
}
