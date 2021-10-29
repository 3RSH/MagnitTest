import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;

@Root(name = "entry")
public class Entry {

  @Attribute
  public int field;
}