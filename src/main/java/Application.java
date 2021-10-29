import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

public class Application {

  private static final InnerData data = new InnerData();

  public static void main(String[] args) {

    long start = System.currentTimeMillis();
    long timeInit;
    long timeSQL;
    long time1xml = 0;
    long time2xml;
    long timeSum;
    long timeAll;
    long sum = 0;

    readInnerData("data.properties");
    timeInit = System.currentTimeMillis() - start;
    System.out.println(
        "Инициализация входных данных выполнена. Прошло " + timeInit / 1000 + " сек.");

    try (Connection connection = DriverManager.getConnection(
        data.getUrlDb(), data.getUserDb(), data.getPasswordDb())) {

      Statement statement = connection.createStatement();

      checkAndCleanDB(statement);
      fillDB(statement);
      timeSQL = System.currentTimeMillis() - (timeInit + start);
      System.out.println("SQL-таблица заполнена. Прошло ещё " + timeSQL / 1000 + " сек.");

      createXml(statement);
      time1xml = System.currentTimeMillis() - (timeSQL + start);
      System.out.println("1.xml создан. Прошло ещё " + time1xml / 1000 + " сек.");

    } catch (SQLException e) {
      System.err.println("Нет соединения с базой данных");
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {

      File transXml = transformXmlToXml("TransformXML.xsl");
      time2xml = System.currentTimeMillis() - (time1xml + start);
      System.out.println("2.xml создан. Прошло ещё " + time2xml / 1000 + " сек.");

      Entries entries = parseXML(transXml);

      for (Entry e : entries.entries) {
        sum += e.field;
      }

      System.out.println("Сумма всех атрибутов fileld из файла 2.xml: " + sum);

      timeSum = System.currentTimeMillis() - (time2xml + start);
      System.out.println("Сумма подсчитана. Прошло ещё " + timeSum / 1000 + " сек.");

    } catch (Exception e) {
      e.printStackTrace();
    }

    timeAll = System.currentTimeMillis() - start;
    System.out.println("Всего прошло " + timeAll / 1000 + " сек.");

    checkSum(sum);
  }

  private static void readInnerData(String propFile) {
    File inData = new File(propFile);

    try (BufferedReader reader = new BufferedReader(new FileReader(inData))) {

      for (int i = 0; i < 4; i++) {
        String line = reader.readLine();

        switch (line.split("=")[0]) {

          case "jdbc.url":
            data.setUrlDb(line.split("=")[1]);
            break;

          case "jdbc.login":
            data.setUserDb(line.split("=")[1]);
            break;

          case "jdbc.password":
            data.setPasswordDb(line.split("=")[1]);
            break;

          case "N":
            data.setN(Integer.parseInt(line.split("=")[1]));
            break;

          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (FileNotFoundException e) {
      System.err.println("Файл с исходными данными не найден.");
    } catch (IllegalArgumentException e) {
      System.err.println("Не найдены исходные данные.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void checkAndCleanDB(Statement statement) throws SQLException {
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM \"TEST\"");
    resultSet.next();

    int size = Integer.parseInt(resultSet.getString(1));

    if (size > 0) {
      statement.execute("TRUNCATE \"TEST\"");
    }
  }

  private static void fillDB(Statement statement) throws SQLException {
    StringBuilder query = new StringBuilder().append("INSERT INTO \"TEST\" VALUES (");

    for (int i = 0; i < data.getN(); i++) {
      int field = (i + 1);

      query.append(field).append(")");

      if (field < data.getN()) {
        query.append(", (");
      }
    }

    statement.execute(query.toString());
  }

  private static void createXml(Statement statement) throws IOException, SQLException {
    File file1 = new File("1.xml");
    file1.createNewFile();

    FileWriter writer = new FileWriter(file1, false);
    writer.write("<entries>\n");

    ResultSet resultSet = statement.executeQuery("SELECT * FROM \"TEST\"");

    while (resultSet.next()) {
      writer.write("  <entry>\n");
      writer.write("    <field>" + resultSet.getString("field") + "</field>\n");
      writer.write("  </entry>\n");
    }

    writer.write("</entries>\n");
    writer.flush();
  }

  private static File transformXmlToXml(String styleSheetFile) throws TransformerException {
    TransformerFactory factory = TransformerFactory.newInstance();
    StreamSource xlsStream = new StreamSource(
        Application.class.getResource(styleSheetFile).toExternalForm());

    StreamSource in = new StreamSource("1.xml");
    File resultXml = new File("2.xml");

    try {
      resultXml.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    StreamResult out = new StreamResult(resultXml);
    Transformer transformer = factory.newTransformer(xlsStream);
    transformer.transform(in, out);

    return resultXml;
  }

  private static Entries parseXML(File sourceXml) throws Exception {
    Serializer serializer = new Persister();
    return serializer.read(Entries.class, sourceXml);
  }

  private static void checkSum(long sum) {
    long checkSum = 0;

    for (int i = 1; i <= data.getN(); i++) {
      checkSum += i;
    }

    if (sum == checkSum) {
      System.out.println("Сумма подсчитана верно.");
    } else {
      System.out.println("Сумма подсчитана неверно.");
    }
  }
}