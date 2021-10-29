import java.util.List;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

@Root(name = "entries")
public class Entries {

  @ElementList(name = "entry", inline = true)
  public List<Entry> entries;
}