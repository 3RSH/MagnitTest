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
    long time2xml = 0;
    long timeSum;
    long timeAll;


    File inData = new File("data.properties");

    try (BufferedReader reader = new BufferedReader(new FileReader(inData))){

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
    }catch (IllegalArgumentException e) {
      System.err.println("Не найдены исходные данные.");
    } catch (IOException e) {
      e.printStackTrace();
    }

    timeInit = System.currentTimeMillis() - start;
    System.out.println("Инициализация входных данных выполнена. Прошло " + timeInit/1000 + " сек.");

    //    ==============

    try (Connection connection = DriverManager.getConnection(
        data.getUrlDb(), data.getUserDb(), data.getPasswordDb())){

      Statement statement = connection.createStatement();

      //    ==============

      ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM \"TEST\"");
      resultSet.next();

      int size = Integer.parseInt(resultSet.getString(1));

      if (size > 0) {
        statement.execute("TRUNCATE \"TEST\"");
      }

      //    ==============

      StringBuilder query = new StringBuilder().append("INSERT INTO \"TEST\" VALUES (");

      for (int i = 0; i < data.getN(); i++) {
        int field = (i + 1);

        query.append(field).append(")");

        if (field < data.getN()) {
          query.append(", (");
        }
      }

      statement.execute(query.toString());

      timeSQL = System.currentTimeMillis() - (timeInit + start);
      System.out.println("SQL-таблица заполнена. Прошло ещё " + timeSQL/1000 + " сек.");

      //    ==============

      File file1 = new File("1.xml");
      file1.createNewFile();

      FileWriter writer = new FileWriter(file1, false);
      writer.write("<entries>\n");

      resultSet = statement.executeQuery("SELECT * FROM \"TEST\"");

      while (resultSet.next()){
        writer.write("  <entry>\n");
        writer.write("    <field>" + resultSet.getString("field") + "</field>\n");
        writer.write("  </entry>\n");
      }

      writer.write("</entries>\n");
      writer.flush();

      time1xml = System.currentTimeMillis() - (timeSQL + start);
      System.out.println("1.xml создан. Прошло ещё " + time1xml/1000 + " сек.");
    } catch (SQLException e) {
      System.err.println("Нет соединения с базой данных");
    } catch (IOException e) {
      e.printStackTrace();
    }

    //    ==============

    TransformerFactory factory = TransformerFactory.newInstance();
    StreamSource xlsStream = new StreamSource(
        Application.class.getResource("TransformXML.xsl").toExternalForm());

    StreamSource in = new StreamSource("1.xml");

    File file2 = new File("2.xml");

    try {
      file2.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    StreamResult out = new StreamResult(file2);

    try {
      Transformer transformer = factory.newTransformer(xlsStream);

      transformer.transform(in, out);

      Serializer serializer = new Persister();
      Entries entries = serializer.read(Entries.class, file2);

      time2xml = System.currentTimeMillis() - (time1xml + start);
      System.out.println("2.xml создан. Прошло ещё " + time2xml/1000 + " сек.");

      //    ==============

      int sum = 0;

      for (Entry e : entries.entries) {
        sum += e.field;
      }

      System.out.println(sum);
    } catch (Exception e) {
      e.printStackTrace();
    }

    timeSum = System.currentTimeMillis() - (time2xml + start);
    System.out.println("Сумма подсчитана. Прошло ещё " + timeSum/1000 + " сек.");

    //    ==============

    timeAll = System.currentTimeMillis() - start;
    System.out.println("Всего прошло " + timeAll/1000 + " сек.");

    //    ==============

    int sum = 0;

    for (int i = 1; i <= data.getN(); i++) {
      sum += i;
    }

    System.out.println("Проверочная сумма: " + sum);
  }
}