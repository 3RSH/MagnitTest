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

  private static final String INNER_DATA_FILENAME = "data.properties";
  private static final String XML_STYLESHEET_FILENAME = "TransformXML.xsl";
  private static final String FIRST_XML_FILENAME = "1.xml";
  private static final String SECOND_XML_FILENAME = "2.xml";

  private static final String DB_URL_PROPERTY_NAME = "jdbc.url";
  private static final String DB_USER_PROPERTY_NAME = "jdbc.login";
  private static final String DB_PASSWORD_PROPERTY_NAME = "jdbc.password";
  private static final String NOTE_COUNT_PROPERTY_NAME = "N";
  public static final String PROPERTIES_VALUES_SPLITTER = "=";

  public static final String GET_DB_NOTE_COUNT_QUERY = "SELECT COUNT(*) FROM \"TEST\"";
  public static final String CLEAN_DB_QUERY = "TRUNCATE \"TEST\"";
  public static final String FILL_DB_QUERY_START = "INSERT INTO \"TEST\" VALUES (";
  public static final String FILL_DB_QUERY_END = ")";
  public static final String FILL_DB_QUERY_CONTINUE = ", (";
  public static final String GET_ALL_DB_NOTES_QUERY = "SELECT * FROM \"TEST\"";

  public static final String DB_COLUMN_LABEL = "field";

  public static final String XML_ROOT_START = "<entries>\n";
  public static final String XML_LVL_1_START = "  <entry>\n";
  public static final String XML_LVL_2_START = "    <field>";
  public static final String XML_LVL_2_END = "</field>\n";
  public static final String XML_LVL_1_END = "  </entry>\n";
  public static final String XML_ROOT_END = "</entries>\n";

  public static final String INIT_MESSAGE = "Инициализация входных данных выполнена. Прошло ";
  public static final String SQL_FILL_MESSAGE = "SQL-таблица заполнена. Прошло ещё ";
  public static final String XML_CREATE_MESSAGE = " создан. Прошло ещё ";
  public static final String RESULT_MESSAGE = "Сумма всех атрибутов %s из файла %s: %d%n";
  public static final String CALCULATE_SUM_MESSAGE = "Сумма подсчитана. Прошло ещё ";
  public static final String COMPLETE_MESSAGE = "Всего прошло ";
  public static final String CHECK_MESSAGE_OK = "Сумма подсчитана верно.";
  public static final String CHECK_MESSAGE_FAIL = "Сумма подсчитана неверно.";

  public static final String SQL_CONNECT_ERROR = "Нет соединения с базой данных";
  public static final String PROPERTY_FILE_ERROR = "Файл с исходными данными не найден.";
  public static final String PROPERTY_ERROR = "Не найдены исходные данные.";

  public static final String TIME_MEASURE = " сек.";
  public static final int TIME_DIVIDER = 1000;

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

    readInnerData(INNER_DATA_FILENAME);
    timeInit = System.currentTimeMillis() - start;
    System.out.println(INIT_MESSAGE + timeInit / TIME_DIVIDER + TIME_MEASURE);

    try (Connection connection = DriverManager.getConnection(
        data.getUrlDb(), data.getUserDb(), data.getPasswordDb())) {

      Statement statement = connection.createStatement();

      checkAndCleanDB(statement);
      fillDB(statement);
      timeSQL = System.currentTimeMillis() - (timeInit + start);
      System.out.println(SQL_FILL_MESSAGE + timeSQL / TIME_DIVIDER + TIME_MEASURE);

      createXml(statement);
      time1xml = System.currentTimeMillis() - (timeSQL + start);
      System.out.println(
          FIRST_XML_FILENAME + XML_CREATE_MESSAGE + time1xml / TIME_DIVIDER + TIME_MEASURE);

    } catch (SQLException e) {
      System.err.println(SQL_CONNECT_ERROR);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {

      File transXml = transformXmlToXml(XML_STYLESHEET_FILENAME);
      time2xml = System.currentTimeMillis() - (time1xml + start);
      System.out.println(
          SECOND_XML_FILENAME + XML_CREATE_MESSAGE + time2xml / TIME_DIVIDER + TIME_MEASURE);

      Entries entries = parseXML(transXml);

      for (Entry e : entries.entries) {
        sum += e.field;
      }

      System.out.printf(RESULT_MESSAGE, DB_COLUMN_LABEL, SECOND_XML_FILENAME, sum);

      timeSum = System.currentTimeMillis() - (time2xml + start);
      System.out.println(CALCULATE_SUM_MESSAGE + timeSum / TIME_DIVIDER + TIME_MEASURE);

    } catch (Exception e) {
      e.printStackTrace();
    }

    timeAll = System.currentTimeMillis() - start;
    System.out.println(COMPLETE_MESSAGE + timeAll / TIME_DIVIDER + TIME_MEASURE);

    checkSum(sum);
  }

  private static void readInnerData(String propFile) {
    File inData = new File(propFile);

    try (BufferedReader reader = new BufferedReader(new FileReader(inData))) {

      for (int i = 0; i < 4; i++) {
        String line = reader.readLine();

        switch (line.split(PROPERTIES_VALUES_SPLITTER)[0]) {

          case DB_URL_PROPERTY_NAME:
            data.setUrlDb(line.split(PROPERTIES_VALUES_SPLITTER)[1]);
            break;

          case DB_USER_PROPERTY_NAME:
            data.setUserDb(line.split(PROPERTIES_VALUES_SPLITTER)[1]);
            break;

          case DB_PASSWORD_PROPERTY_NAME:
            data.setPasswordDb(line.split(PROPERTIES_VALUES_SPLITTER)[1]);
            break;

          case NOTE_COUNT_PROPERTY_NAME:
            data.setN(Integer.parseInt(line.split(PROPERTIES_VALUES_SPLITTER)[1]));
            break;

          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (FileNotFoundException e) {
      System.err.println(PROPERTY_FILE_ERROR);
    } catch (IllegalArgumentException e) {
      System.err.println(PROPERTY_ERROR);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void checkAndCleanDB(Statement statement) throws SQLException {
    ResultSet resultSet = statement.executeQuery(GET_DB_NOTE_COUNT_QUERY);
    resultSet.next();

    int size = Integer.parseInt(resultSet.getString(1));

    if (size > 0) {
      statement.execute(CLEAN_DB_QUERY);
    }
  }

  private static void fillDB(Statement statement) throws SQLException {
    StringBuilder query = new StringBuilder().append(FILL_DB_QUERY_START);

    for (int i = 0; i < data.getN(); i++) {
      int field = (i + 1);

      query.append(field).append(FILL_DB_QUERY_END);

      if (field < data.getN()) {
        query.append(FILL_DB_QUERY_CONTINUE);
      }
    }

    statement.execute(query.toString());
  }

  private static void createXml(Statement statement) throws IOException, SQLException {
    File file1 = new File(FIRST_XML_FILENAME);
    file1.createNewFile();

    FileWriter writer = new FileWriter(file1, false);
    writer.write(XML_ROOT_START);

    ResultSet resultSet = statement.executeQuery(GET_ALL_DB_NOTES_QUERY);

    while (resultSet.next()) {
      writer.write(XML_LVL_1_START);
      writer.write(XML_LVL_2_START);
      writer.write(resultSet.getString(DB_COLUMN_LABEL));
      writer.write(XML_LVL_2_END);
      writer.write(XML_LVL_1_END);
    }

    writer.write(XML_ROOT_END);
    writer.flush();
  }

  private static File transformXmlToXml(String styleSheetFile) throws TransformerException {
    TransformerFactory factory = TransformerFactory.newInstance();
    StreamSource xlsStream = new StreamSource(
        Application.class.getResource(styleSheetFile).toExternalForm());

    StreamSource in = new StreamSource(FIRST_XML_FILENAME);
    File resultXml = new File(SECOND_XML_FILENAME);

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
      System.out.println(CHECK_MESSAGE_OK);
    } else {
      System.out.println(CHECK_MESSAGE_FAIL);
    }
  }
}