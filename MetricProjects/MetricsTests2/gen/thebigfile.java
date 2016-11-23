package smallsql.database;
final class ExpressionFunctionCot extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.COT; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return 1/Math.tan( param1.getDouble() );
}
}
package smallsql.database;
final class ExpressionFunctionMinute extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.MINUTE;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
DateTime.Details details = new DateTime.Details(param1.getLong());
return details.minute;
}
}
package smallsql.database;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
public class FileIndexNode extends IndexNode {
private final FileChannel file;
private long fileOffset;
FileIndexNode(boolean unique, char digit, FileChannel file){
super(unique, digit);
this.file = file;
fileOffset = -1;
}
@Override
protected IndexNode createIndexNode(boolean unique, char digit){
return new FileIndexNode(unique, digit, file);
}
void save() throws SQLException{
StorePage storePage = new StorePage( null, -1, file, fileOffset);
StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.INSERT, fileOffset);
save(store);
fileOffset = store.writeFinsh(null);
}
@Override
void saveRef(StoreImpl output) throws SQLException{
if(fileOffset < 0){
save();
}
output.writeLong(fileOffset);
}
@Override
IndexNode loadRef( long offset ) throws SQLException{
StorePage storePage = new StorePage( null, -1, file, offset);
StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.INSERT, fileOffset);
MemoryStream input = new MemoryStream();
FileIndexNode node = new FileIndexNode( getUnique(), (char)input.readShort(), file );
node.fileOffset = offset;
node.load( store );
return node;
}
static FileIndexNode loadRootNode(boolean unique, FileChannel file, long offset) throws Exception{
StorePage storePage = new StorePage( null, -1, file, offset);
StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.SELECT, offset);
FileIndexNode node = new FileIndexNode( unique, (char)store.readShort(), file );
node.fileOffset = offset;
node.load( store );
return node;
}
}
package smallsql.database;
final class MutableDouble extends Number implements Mutable{
double value;
MutableDouble(double value){
this.value = value;
}
public double doubleValue() {
return value;
}
public float floatValue() {
return (float)value;
}
public int intValue() {
return (int)value;
}
public long longValue() {
return (long)value;
}
public String toString(){
return String.valueOf(value);
}
public Object getImmutableObject(){
return new Double(value);
}
}
package smallsql.database;
public class ExpressionName extends Expression {
private String tableAlias;
private DataSource fromEntry;
private int colIdx;
private TableView table;
private Column column;
ExpressionName(String name){
super(NAME);
setName( name );
}
ExpressionName(int type){
super(type);
}
void setNameAfterTableAlias(String name){
tableAlias = getName();
setName( name );
}
public boolean equals(Object expr){
if(!super.equals(expr)) return false;
if(!(expr instanceof ExpressionName)) return false;
if( ((ExpressionName)expr).fromEntry != fromEntry) return false;
return true;
}
boolean isNull() throws Exception{
return fromEntry.isNull(colIdx);
}
boolean getBoolean() throws Exception{
return fromEntry.getBoolean(colIdx);
}
int getInt() throws Exception{
return fromEntry.getInt(colIdx);
}
long getLong() throws Exception{
return fromEntry.getLong(colIdx);
}
float getFloat() throws Exception{
return fromEntry.getFloat(colIdx);
}
double getDouble() throws Exception{
return fromEntry.getDouble(colIdx);
}
long getMoney() throws Exception{
return fromEntry.getMoney(colIdx);
}
MutableNumeric getNumeric() throws Exception{
return fromEntry.getNumeric(colIdx);
}
Object getObject() throws Exception{
return fromEntry.getObject(colIdx);
}
String getString() throws Exception{
return fromEntry.getString(colIdx);
}
byte[] getBytes() throws Exception{
return fromEntry.getBytes(colIdx);
}
int getDataType(){
switch(getType()){
case NAME:
case GROUP_BY:
return fromEntry.getDataType(colIdx);
case FIRST:
case LAST:
case MAX:
case MIN:
case SUM:
return getParams()[0].getDataType();
case COUNT:
return SQLTokenizer.INT;
default: throw new Error();
}
}
void setFrom( DataSource fromEntry, int colIdx, TableView table ){
this.fromEntry  = fromEntry;
this.colIdx     = colIdx;
this.table      = table;
this.column		= table.columns.get(colIdx);
}
void setFrom( DataSource fromEntry, int colIdx, Column column ){
this.fromEntry  = fromEntry;
this.colIdx     = colIdx;
this.column		= column;
}
DataSource getDataSource(){
return fromEntry;
}
String getTableAlias(){ return tableAlias; }
final TableView getTable(){
return table;
}
final int getColumnIndex(){
return colIdx;
}
final Column getColumn(){
return column;
}
final public String toString(){
if(tableAlias == null) return String.valueOf(getAlias());
return tableAlias + "." + getAlias();
}
String getTableName(){
if(table != null){
return table.getName();
}
return null;
}
int getPrecision(){
return column.getPrecision();
}
int getScale(){
return column.getScale();
}
int getDisplaySize(){
return column.getDisplaySize();
}
boolean isAutoIncrement(){
return column.isAutoIncrement();
}
boolean isCaseSensitive(){
return column.isCaseSensitive();
}
boolean isNullable(){
return column.isNullable();
}
boolean isDefinitelyWritable(){
return true;
}
}
package smallsql.database;
final class ExpressionFunctionDayOfWeek extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.DAYOFWEEK;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
return DateTime.dayOfWeek(param1.getLong())+1;
}
}
package smallsql.database;
final class ExpressionFunctionDegrees extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.DEGREES; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.toDegrees( param1.getDouble() );
}
}
package smallsql.database;
import java.sql.*;
import java.math.*;
import java.util.Map;
import java.util.Calendar;
import java.net.URL;
import java.io.*;
import smallsql.database.language.Language;
public class SSCallableStatement extends SSPreparedStatement implements CallableStatement {
private boolean wasNull;
SSCallableStatement( SSConnection con, String sql ) throws SQLException {
super( con, sql );
}
SSCallableStatement( SSConnection con, String sql, int rsType, int rsConcurrency ) throws SQLException {
super( con, sql, rsType, rsConcurrency );
}
private Expression getValue(int i) throws SQLException{
throw new java.lang.UnsupportedOperationException("Method findParameter() not yet implemented.");
}
public void registerOutParameter(int i, int sqlType) throws SQLException {
throw new java.lang.UnsupportedOperationException("Method registerOutParameter() not yet implemented.");
}
public boolean wasNull(){
return wasNull;
}
public String getString(int i) throws SQLException {
try{
String obj = getValue(i).getString();
wasNull = obj == null;
return obj;
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public boolean getBoolean(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
return expr.getBoolean();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public byte getByte(int i) throws SQLException {
return (byte)getInt( i );
}
public short getShort(int i) throws SQLException {
return (byte)getInt( i );
}
public int getInt(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
return expr.getInt();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public long getLong(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
return expr.getLong();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public float getFloat(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
return expr.getFloat();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public double getDouble(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
return expr.getLong();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public BigDecimal getBigDecimal(int i, int scale) throws SQLException {
try{
MutableNumeric obj = getValue(i).getNumeric();
wasNull = obj == null;
if(wasNull) return null;
return obj.toBigDecimal(scale);
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public byte[] getBytes(int i) throws SQLException {
try{
byte[] obj = getValue(i).getBytes();
wasNull = obj == null;
return obj;
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Date getDate(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
if(wasNull) return null;
return DateTime.getDate( expr.getLong() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Time getTime(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
if(wasNull) return null;
return DateTime.getTime( expr.getLong() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Timestamp getTimestamp(int i) throws SQLException {
try{
Expression expr = getValue(i);
wasNull = expr.isNull();
if(wasNull) return null;
return DateTime.getTimestamp( expr.getLong() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Object getObject(int i) throws SQLException {
try{
Object obj = getValue(i).getObject();
wasNull = obj == null;
return obj;
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public BigDecimal getBigDecimal(int i) throws SQLException {
try{
MutableNumeric obj = getValue(i).getNumeric();
wasNull = obj == null;
if(wasNull) return null;
return obj.toBigDecimal();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Object getObject(int i, Map map) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Method getRef() not yet implemented.");
}
public Blob getBlob(int i) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Method getClob() not yet implemented.");
}
public Array getArray(int i) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Method getDate() not yet implemented.");
}
public Time getTime(int i, Calendar cal) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Method getTimestamp() not yet implemented.");
}
public void registerOutParameter(int i, int sqlType, String typeName) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Method getURL() not yet implemented.");
}
public void setURL(String parameterName, URL x) throws SQLException {
setURL( findParameter( parameterName ), x );
}
public void setNull(String parameterName, int sqlType) throws SQLException {
setNull( findParameter( parameterName ), sqlType );
}
public void setBoolean(String parameterName, boolean x) throws SQLException {
setBoolean( findParameter( parameterName ), x );
}
public void setByte(String parameterName, byte x) throws SQLException {
setByte( findParameter( parameterName ), x );
}
public void setShort(String parameterName, short x) throws SQLException {
setShort( findParameter( parameterName ), x );
}
public void setInt(String parameterName, int x) throws SQLException {
setInt( findParameter( parameterName ), x );
}
public void setLong(String parameterName, long x) throws SQLException {
setLong( findParameter( parameterName ), x );
}
public void setFloat(String parameterName, float x) throws SQLException {
setFloat( findParameter( parameterName ), x );
}
public void setDouble(String parameterName, double x) throws SQLException {
setDouble( findParameter( parameterName ), x );
}
public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
setBigDecimal( findParameter( parameterName ), x );
}
public void setString(String parameterName, String x) throws SQLException {
setString( findParameter( parameterName ), x );
}
public void setBytes(String parameterName, byte[] x) throws SQLException {
setBytes( findParameter( parameterName ), x );
}
public void setDate(String parameterName, Date x) throws SQLException {
setDate( findParameter( parameterName ), x );
}
public void setTime(String parameterName, Time x) throws SQLException {
setTime( findParameter( parameterName ), x );
}
public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
setTimestamp( findParameter( parameterName ), x );
}
public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
setAsciiStream( findParameter( parameterName ), x, length );
}
public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
setBinaryStream( findParameter( parameterName ), x, length );
}
public void setObject(String parameterName, Object x, int sqlType, int scale) throws SQLException {
setObject( findParameter( parameterName ), x, sqlType, scale );
}
public void setObject(String parameterName, Object x, int sqlType) throws SQLException {
setObject( findParameter( parameterName ), x, sqlType );
}
public void setObject(String parameterName, Object x) throws SQLException {
setObject( findParameter( parameterName ), x );
}
public void setCharacterStream(String parameterName, Reader x, int length) throws SQLException {
setCharacterStream( findParameter( parameterName ), x, length );
}
public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
setDate( findParameter( parameterName ), x, cal );
}
public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
setTime( findParameter( parameterName ), x, cal );
}
public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
setTimestamp( findParameter( parameterName ), x, cal );
}
public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
setNull( findParameter( parameterName ), sqlType, typeName );
}
public String getString(String parameterName) throws SQLException {
return getString( findParameter( parameterName ) );
}
public boolean getBoolean(String parameterName) throws SQLException {
return getBoolean( findParameter( parameterName ) );
}
public byte getByte(String parameterName) throws SQLException {
return getByte( findParameter( parameterName ) );
}
public short getShort(String parameterName) throws SQLException {
return getShort( findParameter( parameterName ) );
}
public int getInt(String parameterName) throws SQLException {
return getInt( findParameter( parameterName ) );
}
public long getLong(String parameterName) throws SQLException {
return getLong( findParameter( parameterName ) );
}
public float getFloat(String parameterName) throws SQLException {
return getFloat( findParameter( parameterName ) );
}
public double getDouble(String parameterName) throws SQLException {
return getDouble( findParameter( parameterName ) );
}
public byte[] getBytes(String parameterName) throws SQLException {
return getBytes( findParameter( parameterName ) );
}
public Date getDate(String parameterName) throws SQLException {
return getDate( findParameter( parameterName ) );
}
public Time getTime(String parameterName) throws SQLException {
return getTime( findParameter( parameterName ) );
}
public Timestamp getTimestamp(String parameterName) throws SQLException {
return getTimestamp( findParameter( parameterName ) );
}
public Object getObject(String parameterName) throws SQLException {
return getObject( findParameter( parameterName ) );
}
public BigDecimal getBigDecimal(String parameterName) throws SQLException {
return getBigDecimal( findParameter( parameterName ) );
}
public Object getObject(String parameterName, Map map) throws SQLException {
return getObject( findParameter( parameterName ), map );
}
public Ref getRef(String parameterName) throws SQLException {
return getRef( findParameter( parameterName ) );
}
public Blob getBlob(String parameterName) throws SQLException {
return getBlob( findParameter( parameterName ) );
}
public Clob getClob(String parameterName) throws SQLException {
return getClob( findParameter( parameterName ) );
}
public Array getArray(String parameterName) throws SQLException {
return getArray( findParameter( parameterName ) );
}
public Date getDate(String parameterName, Calendar cal) throws SQLException {
return getDate( findParameter( parameterName ), cal );
}
public Time getTime(String parameterName, Calendar cal) throws SQLException {
return getTime( findParameter( parameterName ), cal );
}
public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
return getTimestamp( findParameter( parameterName ), cal );
}
public URL getURL(String parameterName) throws SQLException {
return getURL( findParameter( parameterName ) );
}
}
package smallsql.database;
final class ExpressionFunctionCos extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.COS; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.cos( param1.getDouble() );
}
}
package smallsql.database;
import java.sql.SQLException;
import smallsql.database.language.Language;
final class CommandTable extends Command{
final private Columns columns = new Columns();
final private IndexDescriptions indexes = new IndexDescriptions();
final private ForeignKeys foreignKeys = new ForeignKeys();
final private int tableCommandType;
CommandTable( Logger log, String catalog, String name, int tableCommandType ){
super(log);
this.type = SQLTokenizer.TABLE;
this.catalog = catalog;
this.name = name;
this.tableCommandType = tableCommandType;
}
void addColumn(Column column) throws SQLException{
addColumn(columns, column);
}
void addIndex( IndexDescription indexDescription ) throws SQLException{
indexes.add(indexDescription);
}
void addForeingnKey(ForeignKey key){
foreignKeys.add(key);
}
void executeImpl(SSConnection con, SSStatement st) throws Exception{
Database database = catalog == null ?
con.getDatabase(false) :
Database.getDatabase( catalog, con, false );
switch(tableCommandType){
case SQLTokenizer.CREATE:
database.createTable( con, name, columns, indexes, foreignKeys );
break;
case SQLTokenizer.ADD:
con = new SSConnection(con);
Table oldTable = (Table)database.getTableView( con, name);
TableStorePage tableLock = oldTable.requestLock( con, SQLTokenizer.ALTER, -1);
String newName = "#" + System.currentTimeMillis() + this.hashCode();
try{
Columns oldColumns = oldTable.columns;
Columns newColumns = oldColumns.copy();
for(int i = 0; i < columns.size(); i++){
addColumn(newColumns, columns.get(i));
}
Table newTable = database.createTable( con, newName, newColumns, oldTable.indexes, indexes, foreignKeys );
StringBuffer buffer = new StringBuffer(256);
buffer.append("INSERT INTO ").append( newName ).append( '(' );
for(int c=0; c<oldColumns.size(); c++){
if(c != 0){
buffer.append( ',' );
}
buffer.append( oldColumns.get(c).getName() );
}
buffer.append( ")  SELECT * FROM " ).append( name );
con.createStatement().execute( buffer.toString() );
database.replaceTable( oldTable, newTable );
}catch(Exception ex){
try {
database.dropTable(con, newName);
} catch (Exception ex1) {
try{
indexes.drop(database);
} catch (Exception ex1) {
throw ex;
}finally{
tableLock.freeLock();
}
break;
default:
throw new Error();
}
}
private void addColumn(Columns cols, Column column) throws SQLException{
if(cols.get(column.getName()) != null){
throw SmallSQLException.create(Language.COL_DUPLICATE, column.getName());
}
cols.add(column);
}
}
package smallsql.junit;
import java.sql.*;
import java.util.ArrayList;
public class TestThreads extends BasicTestCase{
volatile Throwable throwable;
public void testConcurrentRead() throws Throwable{
ArrayList threadList = new ArrayList();
throwable = null;
final String sql = "Select * From table_OrderBy1";
final Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery("Select * From table_OrderBy1");
int count = 0;
while(rs.next()){
count++;
}
final int rowCount = count;
for(int i = 0; i < 200; i++){
Thread thread = new Thread(new Runnable(){
public void run(){
try{
assertRowCount(rowCount, sql);
}catch(Throwable ex){
throwable = ex;
}
}
});
threadList.add(thread);
thread.start();
}
for(int i = 0; i < threadList.size(); i++){
Thread thread = (Thread)threadList.get(i);
thread.join(5000);
}
if(throwable != null){
throw throwable;
}
}
public void testConcurrentThreadWrite() throws Throwable{
ArrayList threadList = new ArrayList();
throwable = null;
final Connection con = AllTests.getConnection();
Statement st = con.createStatement();
try{
st.execute("CREATE TABLE ConcurrentWrite( value int)");
st.execute("INSERT INTO ConcurrentWrite(value) Values(0)");
for(int i = 0; i < 200; i++){
Thread thread = new Thread(new Runnable(){
public void run(){
try{
Statement st2 = con.createStatement();
int count = st2.executeUpdate("UPDATE ConcurrentWrite SET value = value + 1");
assertEquals("Update Count", 1, count);
}catch(Throwable ex){
throwable = ex;
}
}
});
threadList.add(thread);
thread.start();
}
for(int i = 0; i < threadList.size(); i++){
Thread thread = (Thread)threadList.get(i);
thread.join(5000);
}
if(throwable != null){
throw throwable;
}
assertEqualsRsValue(new Integer(200), "SELECT value FROM ConcurrentWrite");
}finally{
dropTable(con, "ConcurrentWrite");
}
}
public void testConcurrentConnectionWrite() throws Throwable{
ArrayList threadList = new ArrayList();
throwable = null;
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
try{
st.execute("CREATE TABLE ConcurrentWrite( value int)");
st.execute("INSERT INTO ConcurrentWrite(value) Values(0)");
for(int i = 0; i < 200; i++){
Thread thread = new Thread(new Runnable(){
public void run(){
try{
Connection con2 = AllTests.createConnection();
Statement st2 = con2.createStatement();
int count = st2.executeUpdate("UPDATE ConcurrentWrite SET value = value + 1");
assertEquals("Update Count", 1, count);
con2.close();
}catch(Throwable ex){
throwable = ex;
}
}
});
threadList.add(thread);
thread.start();
}
for(int i = 0; i < threadList.size(); i++){
Thread thread = (Thread)threadList.get(i);
thread.join(5000);
}
if(throwable != null){
throw throwable;
}
assertEqualsRsValue(new Integer(200), "SELECT value FROM ConcurrentWrite");
}finally{
dropTable(con, "ConcurrentWrite");
}
}
}
package smallsql.database;
import java.sql.*;
class ViewResult extends TableViewResult {
final private View view;
final private Expressions columnExpressions;
final private CommandSelect commandSelect;
ViewResult(View view){
this.view = view;
this.columnExpressions = view.commandSelect.columnExpressions;
this.commandSelect     = view.commandSelect;
}
ViewResult(SSConnection con, CommandSelect commandSelect) throws SQLException{
try{
this.view = new View( con, commandSelect);
this.columnExpressions = commandSelect.columnExpressions;
this.commandSelect     = commandSelect;
}catch(Exception e){
throw SmallSQLException.createFromException(e);
}
}
boolean init( SSConnection con ) throws Exception{
if(super.init(con)){
commandSelect.compile(con);
return true;
}
return false;
}
TableView getTableView(){
return view;
}
void deleteRow() throws SQLException{
commandSelect.deleteRow(con);
}
void updateRow(Expression[] updateValues) throws Exception{
commandSelect.updateRow(con, updateValues);
}
void insertRow(Expression[] updateValues) throws Exception{
commandSelect.insertRow(con, updateValues);
}
boolean isNull(int colIdx) throws Exception {
return columnExpressions.get(colIdx).isNull();
}
boolean getBoolean(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getBoolean();
}
int getInt(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getInt();
}
long getLong(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getLong();
}
float getFloat(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getFloat();
}
double getDouble(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getDouble();
}
long getMoney(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getMoney();
}
MutableNumeric getNumeric(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getNumeric();
}
Object getObject(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getObject();
}
String getString(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getString();
}
byte[] getBytes(int colIdx) throws Exception {
return columnExpressions.get(colIdx).getBytes();
}
int getDataType(int colIdx) {
return columnExpressions.get(colIdx).getDataType();
}
void beforeFirst() throws Exception {
commandSelect.beforeFirst();
}
boolean isBeforeFirst() throws SQLException{
return commandSelect.isBeforeFirst();
}
boolean isFirst() throws SQLException{
return commandSelect.isFirst();
}
boolean first() throws Exception {
return commandSelect.first();
}
boolean previous() throws Exception{
return commandSelect.previous();
}
boolean next() throws Exception {
return commandSelect.next();
}
boolean last() throws Exception{
return commandSelect.last();
}
boolean isLast() throws Exception{
return commandSelect.isLast();
}
boolean isAfterLast() throws Exception{
return commandSelect.isAfterLast();
}
void afterLast() throws Exception{
commandSelect.afterLast();
}
boolean absolute(int row) throws Exception{
return commandSelect.absolute(row);
}
boolean relative(int rows) throws Exception{
return commandSelect.relative(rows);
}
int getRow() throws Exception{
return commandSelect.getRow();
}
long getRowPosition() {
return commandSelect.from.getRowPosition();
}
void setRowPosition(long rowPosition) throws Exception {
commandSelect.from.setRowPosition(rowPosition);
}
final boolean rowInserted(){
return commandSelect.from.rowInserted();
}
final boolean rowDeleted(){
return commandSelect.from.rowDeleted();
}
void nullRow() {
commandSelect.from.nullRow();
}
void noRow() {
commandSelect.from.noRow();
}
final void execute() throws Exception{
commandSelect.from.execute();
}
}
package smallsql.database;
final class ExpressionFunctionIIF extends ExpressionFunction {
int getFunction() {
return SQLTokenizer.IIF;
}
boolean isNull() throws Exception {
if(param1.getBoolean())
return param2.isNull();
return param3.isNull();
}
boolean getBoolean() throws Exception {
if(param1.getBoolean())
return param2.getBoolean();
return param3.getBoolean();
}
int getInt() throws Exception {
if(param1.getBoolean())
return param2.getInt();
return param3.getInt();
}
long getLong() throws Exception {
if(param1.getBoolean())
return param2.getLong();
return param3.getLong();
}
float getFloat() throws Exception {
if(param1.getBoolean())
return param2.getFloat();
return param3.getFloat();
}
double getDouble() throws Exception {
if(param1.getBoolean())
return param2.getDouble();
return param3.getDouble();
}
long getMoney() throws Exception {
if(param1.getBoolean())
return param2.getMoney();
return param3.getMoney();
}
MutableNumeric getNumeric() throws Exception {
if(param1.getBoolean())
return param2.getNumeric();
return param3.getNumeric();
}
Object getObject() throws Exception {
if(param1.getBoolean())
return param2.getObject();
return param3.getObject();
}
String getString() throws Exception {
if(param1.getBoolean())
return param2.getString();
return param3.getString();
}
final int getDataType() {
return ExpressionArithmetic.getDataType(param2, param3);
}
final int getPrecision(){
return Math.max( param2.getPrecision(), param3.getPrecision() );
}
final int getScale(){
return Math.max( param2.getScale(), param3.getScale() );
}
}
package smallsql.database;
import java.sql.*;
public class TableStorePage extends StorePage{
final Table table;
int lockType;
SSConnection con;
TableStorePage nextLock;
TableStorePage(SSConnection con, Table table, int lockType, long fileOffset){
super( null, 0, table.raFile, fileOffset );
this.con 	= con;
this.table = table;
this.lockType 	= lockType;
}
byte[] getData(){
return page;
}
long commit() throws SQLException{
if(nextLock != null){
fileOffset = nextLock.commit();
nextLock = null;
rollback();
return fileOffset;
}
if(lockType == TableView.LOCK_READ)
return fileOffset;
return super.commit();
}
final void freeLock(){
table.freeLock(this);
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
class FileIndex extends Index {
static void print(Index index, Expressions expressions){
IndexScrollStatus scroll = index.createScrollStatus(expressions);
long l;
while((l= scroll.getRowOffset(true)) >=0){
System.out.println(l);
}
System.out.println("============================");
}
private final FileChannel raFile;
FileIndex( boolean unique, FileChannel raFile ) {
this(new FileIndexNode( unique, (char)-1, raFile), raFile);
}
FileIndex( FileIndexNode root, FileChannel raFile ) {
super(root);
this.raFile = raFile;
}
static FileIndex load( FileChannel raFile ) throws Exception{
ByteBuffer buffer = ByteBuffer.allocate(1);
raFile.read(buffer);
buffer.position(0);
boolean unique = buffer.get() != 0;
FileIndexNode root = FileIndexNode.loadRootNode( unique, raFile, raFile.position() );
return new FileIndex( root, raFile );
}
void save() throws Exception{
ByteBuffer buffer = ByteBuffer.allocate(1);
buffer.put(rootPage.getUnique() ? (byte)1 : (byte)0 );
buffer.position(0);
raFile.write( buffer );
((FileIndexNode)rootPage).save();
}
void close() throws IOException{
raFile.close();
}
}
package smallsql.database;
public class ExpressionFunctionLocate extends ExpressionFunctionReturnInt {
int getFunction() {
return SQLTokenizer.LOCATE;
}
boolean isNull() throws Exception {
return param1.isNull() || param2.isNull();
}
int getInt() throws Exception {
String suchstr = param1.getString();
String value   = param2.getString();
if(suchstr == null || value == null || suchstr.length() == 0 || value.length() == 0) return 0;
int start = 0;
if(param3 != null){
start = param3.getInt()-1;
}
return value.toUpperCase().indexOf( suchstr.toUpperCase(), start ) +1;
}
}
package smallsql.database;
public class ExpressionFunctionSpace extends ExpressionFunctionReturnString {
final int getFunction() {
return SQLTokenizer.SPACE;
}
boolean isNull() throws Exception {
return param1.isNull() || param1.getInt()<0;
}
final String getString() throws Exception {
if(isNull()) return null;
int size = param1.getInt();
if(size < 0){
return null;
}
char[] buffer = new char[size];
for(int i=0; i<size; i++){
buffer[i] = ' ';
}
return new String(buffer);
}
final int getDataType() {
return SQLTokenizer.VARCHAR;
}
}
package smallsql.database;
class CommandDelete extends CommandSelect {
CommandDelete(Logger log){
super(log);
}
void executeImpl(SSConnection con, SSStatement st) throws Exception {
compile(con);
TableViewResult result = TableViewResult.getTableViewResult(from);
updateCount = 0;
from.execute();
while(next()){
result.deleteRow();
updateCount++;
}
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
public class ExpressionArithmetic extends Expression {
private Expression left;
private Expression right;
private Expression right2;
private Expression[] inList;
final private int operation;
ExpressionArithmetic( Expression left, int operation){
super(FUNCTION);
this.left  = left;
this.operation = operation;
super.setParams( new Expression[]{ left });
}
ExpressionArithmetic( Expression left, Expression right, int operation){
super(FUNCTION);
this.left   = left;
this.right  = right;
this.operation = operation;
super.setParams( new Expression[]{ left, right });
}
ExpressionArithmetic( Expression left, Expression right, Expression right2, int operation){
super(FUNCTION);
this.left   = left;
this.right  = right;
this.right2 = right2;
this.operation = operation;
super.setParams( new Expression[]{ left, right, right2 });
}
ExpressionArithmetic( Expression left, Expressions inList, int operation){
super(FUNCTION);
this.left   = left;
this.operation = operation;
Expression[] params;
if(inList != null){
this.inList = inList.toArray();
params = new Expression[this.inList.length+1];
params[0] = left;
System.arraycopy(this.inList, 0, params, 1, this.inList.length);
}else{
params = new Expression[]{ left };
}
super.setParams( params );
}
int getOperation(){
return operation;
}
private Expression convertExpressionIfNeeded( Expression expr, Expression other ){
if(expr == null || other == null){
return expr;
}
switch(expr.getDataType()){
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.BINARY:
switch(other.getDataType()){
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.VARBINARY:
ExpressionFunctionRTrim trim = new ExpressionFunctionRTrim();
trim.setParams(new Expression[]{expr});
return trim;
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.BINARY:
if(other.getPrecision() > expr.getPrecision()){
return new ExpressionFunctionConvert(new ColumnExpression(other), expr, null );
}
break;
}
break;
}
return expr;
}
final void setParamAt( Expression param, int idx){
switch(idx){
case 0:
left = param;
break;
case 1:
if(right != null){
right = param;
}
break;
case 2:
if(right != null){
right2 = param;
}
break;
}
if(inList != null && idx>0 && idx<=inList.length){
inList[idx-1] = param;
}
super.setParamAt( param, idx );
}
public boolean equals(Object expr){
if(!super.equals(expr)) return false;
if(!(expr instanceof ExpressionArithmetic)) return false;
if( ((ExpressionArithmetic)expr).operation != operation) return false;
return true;
}
int getInt() throws java.lang.Exception {
if(isNull()) return 0;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? 1 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return getIntImpl();
case SQLTokenizer.BIGINT:
return (int)getLongImpl();
case SQLTokenizer.REAL:
return (int)getFloatImpl();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return (int)getDoubleImpl();
}
throw createUnspportedConversion( SQLTokenizer.INT);
}
private int getIntImpl() throws java.lang.Exception {
switch(operation){
case ADD:       return left.getInt() + right.getInt();
case SUB:       return left.getInt() - right.getInt();
case MUL:       return left.getInt() * right.getInt();
case DIV:       return left.getInt() / right.getInt();
case NEGATIVE:  return               - left.getInt();
case MOD:		return left.getInt() % right.getInt();
case BIT_NOT:   return               ~ left.getInt();
}
throw createUnspportedConversion( SQLTokenizer.INT);
}
long getLong() throws java.lang.Exception {
if(isNull()) return 0;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? 1 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return getIntImpl();
case SQLTokenizer.BIGINT:
return getLongImpl();
case SQLTokenizer.REAL:
return (long)getFloatImpl();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return (long)getDoubleImpl();
}
throw createUnspportedConversion( SQLTokenizer.LONG);
}
private long getLongImpl() throws java.lang.Exception {
if(isNull()) return 0;
switch(operation){
case ADD: return left.getLong() + right.getLong();
case SUB: return left.getLong() - right.getLong();
case MUL: return left.getLong() * right.getLong();
case DIV: return left.getLong() / right.getLong();
case NEGATIVE:  return          - left.getLong();
case MOD:		return left.getLong() % right.getLong();
case BIT_NOT:   return          ~ right.getInt();
}
throw createUnspportedConversion( SQLTokenizer.LONG);
}
double getDouble() throws java.lang.Exception {
if(isNull()) return 0;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? 1 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return getIntImpl();
case SQLTokenizer.BIGINT:
return getLongImpl();
case SQLTokenizer.REAL:
return getFloatImpl();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return getDoubleImpl();
}
throw createUnspportedConversion( SQLTokenizer.DOUBLE);
}
private double getDoubleImpl() throws java.lang.Exception{
if(operation == NEGATIVE)
return getDoubleImpl(0, left.getDouble());
return getDoubleImpl(left.getDouble(), right.getDouble());
}
private double getDoubleImpl( double lVal, double rVal) throws java.lang.Exception{
switch(operation){
case ADD: return lVal + rVal;
case SUB: return lVal - rVal;
case MUL: return lVal * rVal;
case DIV: return lVal / rVal;
case NEGATIVE: return - rVal;
case MOD:		return lVal % rVal;
}
throw createUnspportedConversion( SQLTokenizer.DOUBLE);
}
float getFloat() throws java.lang.Exception {
if(isNull()) return 0;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? 1 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return getIntImpl();
case SQLTokenizer.BIGINT:
return getLongImpl();
case SQLTokenizer.REAL:
return getFloatImpl();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return (float)getDoubleImpl();
}
throw createUnspportedConversion( SQLTokenizer.DOUBLE);
}
private float getFloatImpl() throws java.lang.Exception {
switch(operation){
case ADD: return left.getFloat() + right.getFloat();
case SUB: return left.getFloat() - right.getFloat();
case MUL: return left.getFloat() * right.getFloat();
case DIV: return left.getFloat() / right.getFloat();
case NEGATIVE:  return           - left.getFloat();
case MOD:		return left.getFloat() % right.getFloat();
}
throw createUnspportedConversion( SQLTokenizer.REAL );
}
long getMoney() throws java.lang.Exception {
if(isNull()) return 0;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? 10000 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return getIntImpl() * 10000;
case SQLTokenizer.BIGINT:
return getLongImpl() * 10000;
case SQLTokenizer.REAL:
return Utils.doubleToMoney( getFloatImpl() );
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return Utils.doubleToMoney( getDoubleImpl() );
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return getMoneyImpl();
}
throw createUnspportedConversion( SQLTokenizer.DOUBLE);
}
private long getMoneyImpl() throws java.lang.Exception {
switch(operation){
case ADD: return left.getMoney() + right.getMoney();
case SUB: return left.getMoney() - right.getMoney();
case MUL: return left.getMoney() * right.getMoney() / 10000;
case DIV: return left.getMoney() * 10000 / right.getMoney();
case NEGATIVE: return 			 - left.getMoney();
}
throw createUnspportedConversion( SQLTokenizer.MONEY );
}
MutableNumeric getNumeric() throws java.lang.Exception {
if(isNull()) return null;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return new MutableNumeric(getBoolean() ? 1 : 0);
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return new MutableNumeric(getIntImpl());
case SQLTokenizer.BIGINT:
return new MutableNumeric(getLongImpl());
case SQLTokenizer.REAL:
return new MutableNumeric(getFloatImpl());
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return new MutableNumeric( getDoubleImpl() );
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return getNumericImpl();
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return new MutableNumeric(getMoneyImpl(),4);
}
throw createUnspportedConversion( SQLTokenizer.DOUBLE);
}
private MutableNumeric getNumericImpl() throws java.lang.Exception {
switch(operation){
case ADD:
{
MutableNumeric num = left.getNumeric();
num.add( right.getNumeric() );
return num;
}
case SUB:
{
MutableNumeric num = left.getNumeric();
num.sub( right.getNumeric() );
return num;
}
case MUL:
if(getDataType(right.getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT){
MutableNumeric num = left.getNumeric();
num.mul(right.getInt());
return num;
}else
if(getDataType(left.getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT){
MutableNumeric num = right.getNumeric();
num.mul(left.getInt());
return num;
}else{
MutableNumeric num = left.getNumeric();
num.mul( right.getNumeric() );
return num;
}
case DIV:
{
MutableNumeric num = left.getNumeric();
if(getDataType(right.getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT)
num.div( right.getInt() );
else
num.div( right.getNumeric() );
return num;
}
case NEGATIVE:
{
MutableNumeric num = left.getNumeric();
num.setSignum(-num.getSignum());
return num;
}
case MOD:
{
if(getDataType(getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT)
return new MutableNumeric(getInt());
MutableNumeric num = left.getNumeric();
num.mod( right.getNumeric() );
return num;
}
default:    throw createUnspportedConversion( SQLTokenizer.NUMERIC );
}
}
Object getObject() throws java.lang.Exception {
if(isNull()) return null;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? Boolean.TRUE : Boolean.FALSE;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return getBytes();
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return new Integer( getInt() );
case SQLTokenizer.BIGINT:
return new Long( getLong() );
case SQLTokenizer.REAL:
return new Float( getFloat() );
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return new Double( getDouble() );
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return Money.createFromUnscaledValue( getMoney() );
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return getNumeric();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return getString( left.getString(), right.getString() );
case SQLTokenizer.JAVA_OBJECT:
Object lObj = left.getObject();
Object rObj = right.getObject();
if(lObj instanceof Number && rObj instanceof Number)
return new Double( getDoubleImpl( ((Number)lObj).doubleValue(), ((Number)rObj).doubleValue() ) );
else
return getString( lObj.toString(), rObj.toString() );
case SQLTokenizer.LONGVARBINARY:
return getBytes();
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
return new DateTime( getLong(), dataType );
case SQLTokenizer.UNIQUEIDENTIFIER:
return getBytes();
default: throw createUnspportedDataType();
}
}
boolean getBoolean() throws java.lang.Exception {
switch(operation){
case OR:    return left.getBoolean() || right.getBoolean();
case AND:   return left.getBoolean() && right.getBoolean();
case NOT:   return                      !left.getBoolean();
case LIKE:  return Utils.like( left.getString(), right.getString());
case ISNULL:return 						left.isNull();
case ISNOTNULL:	return 					!left.isNull();
case IN:	if(right == null)
return isInList();
break;
}
final boolean leftIsNull = left.isNull();
int dataType;
if(operation == NEGATIVE || operation == BIT_NOT){
if(leftIsNull) return false;
dataType = left.getDataType();
}else{
final boolean rightIsNull = right.isNull();
if(operation == EQUALS_NULL && leftIsNull && rightIsNull) return true;
if(leftIsNull || rightIsNull) return false;
dataType = getDataType(left, right);
}
switch(dataType){
case SQLTokenizer.BOOLEAN:
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return left.getBoolean() == right.getBoolean();
case UNEQUALS:  return left.getBoolean() != right.getBoolean();
}
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
case SQLTokenizer.BIT:
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return left.getInt() == right.getInt();
case GREATER:   return left.getInt() >  right.getInt();
case GRE_EQU:   return left.getInt() >= right.getInt();
case LESSER:    return left.getInt() <  right.getInt();
case LES_EQU:   return left.getInt() <= right.getInt();
case UNEQUALS:  return left.getInt() != right.getInt();
case BETWEEN:
int _left = left.getInt();
return _left >= right.getInt() && right2.getInt() >= _left;
default:
return getInt() != 0;
}
case SQLTokenizer.BIGINT:
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return left.getLong() == right.getLong();
case GREATER:   return left.getLong() >  right.getLong();
case GRE_EQU:   return left.getLong() >= right.getLong();
case LESSER:    return left.getLong() <  right.getLong();
case LES_EQU:   return left.getLong() <= right.getLong();
case UNEQUALS:  return left.getLong() != right.getLong();
case BETWEEN:
long _left = left.getLong();
return _left >= right.getLong() && right2.getLong() >= _left;
default:
return getLong() != 0;
}
case SQLTokenizer.REAL:
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return left.getFloat() == right.getFloat();
case GREATER:   return left.getFloat() >  right.getFloat();
case GRE_EQU:   return left.getFloat() >= right.getFloat();
case LESSER:    return left.getFloat() <  right.getFloat();
case LES_EQU:   return left.getFloat() <= right.getFloat();
case UNEQUALS:  return left.getFloat() != right.getFloat();
case BETWEEN:
float _left = left.getFloat();
return _left >= right.getFloat() && right2.getFloat() >= _left;
default:
return getFloat() != 0;
}
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return left.getDouble() == right.getDouble();
case GREATER:   return left.getDouble() >  right.getDouble();
case GRE_EQU:   return left.getDouble() >= right.getDouble();
case LESSER:    return left.getDouble() <  right.getDouble();
case LES_EQU:   return left.getDouble() <= right.getDouble();
case UNEQUALS:  return left.getDouble() != right.getDouble();
case BETWEEN:
double _left = left.getDouble();
return _left >= right.getDouble() && right2.getDouble() >= _left;
default:
return getDouble() != 0;
}
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return left.getMoney() == right.getMoney();
case GREATER:   return left.getMoney() >  right.getMoney();
case GRE_EQU:   return left.getMoney() >= right.getMoney();
case LESSER:    return left.getMoney() <  right.getMoney();
case LES_EQU:   return left.getMoney() <= right.getMoney();
case UNEQUALS:  return left.getMoney() != right.getMoney();
case BETWEEN:
long _left = left.getMoney();
return _left >= right.getMoney() && right2.getMoney() >= _left;
default:
return getMoney() != 0;
}
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:{
if(operation == NEGATIVE)
return left.getNumeric().getSignum() != 0;
int comp = left.getNumeric().compareTo( right.getNumeric() );
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return comp == 0;
case GREATER:   return comp >  0;
case GRE_EQU:   return comp >= 0;
case LESSER:    return comp <  0;
case LES_EQU:   return comp <= 0;
case UNEQUALS:  return comp != 0;
case BETWEEN:
return comp >= 0 && 0 >= left.getNumeric().compareTo( right2.getNumeric() );
default:
return getNumeric().getSignum() != 0;
}
}
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.CLOB:{
final String leftStr = left.getString();
final String rightStr = right.getString();
int comp = String.CASE_INSENSITIVE_ORDER.compare( leftStr, rightStr );
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return comp == 0;
case GREATER:   return comp >  0;
case GRE_EQU:   return comp >= 0;
case LESSER:    return comp <  0;
case LES_EQU:   return comp <= 0;
case UNEQUALS:  return comp != 0;
case BETWEEN:
return comp >= 0 && 0 >= String.CASE_INSENSITIVE_ORDER.compare( leftStr, right2.getString() );
case ADD:       return Utils.string2boolean(leftStr + rightStr);
}
break;}
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
case SQLTokenizer.UNIQUEIDENTIFIER:{
byte[] leftBytes = left.getBytes();
byte[] rightBytes= right.getBytes();
int comp = Utils.compareBytes( leftBytes, rightBytes);
switch(operation){
case IN:
case EQUALS_NULL:
case EQUALS:    return comp == 0;
case GREATER:   return comp >  0;
case GRE_EQU:   return comp >= 0;
case LESSER:    return comp <  0;
case LES_EQU:   return comp <= 0;
case UNEQUALS:  return comp != 0;
case BETWEEN:
return comp >= 0 && 0 >= Utils.compareBytes( leftBytes, right2.getBytes() );
}
break;}
}
throw createUnspportedDataType();
}
String getString() throws java.lang.Exception {
if(isNull()) return null;
return getObject().toString();
}
final private String getString( String lVal, String rVal ) throws java.lang.Exception {
switch(operation){
case ADD: return lVal + rVal;
}
throw createUnspportedConversion( SQLTokenizer.VARCHAR );
}
int getDataType() {
switch(operation){
case NEGATIVE:
case BIT_NOT:
return left.getDataType();
case EQUALS:
case EQUALS_NULL:
case GREATER:
case GRE_EQU:
case LESSER:
case LES_EQU:
case UNEQUALS:
case BETWEEN:
case OR:
case AND:
case NOT:
case LIKE:
case ISNULL:
case ISNOTNULL:
return SQLTokenizer.BOOLEAN;
default:
return getDataType(left, right);
}
}
int getScale(){
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:
switch(operation){
case ADD:
case SUB:
return Math.max(left.getScale(), right.getScale());
case MUL:
return left.getScale() + right.getScale();
case DIV:
return Math.max(left.getScale()+5, right.getScale()+4);
case NEGATIVE:
return left.getScale();
case MOD:
return 0;
}
}
return getScale(dataType);
}
boolean isNull() throws Exception{
switch(operation){
case OR:
case AND:
case NOT:
case LIKE:
case ISNULL:
case ISNOTNULL:
case IN:
return false; 
case NEGATIVE:
case BIT_NOT:
return                  left.isNull();
default:       return left.isNull() || right.isNull();
}
}
byte[] getBytes() throws java.lang.Exception {
throw createUnspportedConversion( SQLTokenizer.BINARY );
}
boolean isInList() throws Exception{
if(left.isNull()) return false;
try{
for(int i=0; i<inList.length; i++){
right = inList[i];
if(getBoolean()) return true;
}
}finally{
right = null;
}
return false;
}
SQLException createUnspportedDataType(){
Object[] params = {
SQLTokenizer.getKeyWord(getDataType(left, right)),
getKeywordFromOperation(operation)
};
return SmallSQLException.create(Language.UNSUPPORTED_DATATYPE_OPER, params);
}
SQLException createUnspportedConversion( int dataType ){
int type = left == null ? right.getDataType() : getDataType(left, right);
Object[] params = new Object[] {
SQLTokenizer.getKeyWord(dataType),
SQLTokenizer.getKeyWord(type),
getKeywordFromOperation(operation)
};
return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION_OPER, params);
}
void optimize() throws SQLException{
super.optimize();
Expression[] params = getParams();
if(params.length == 1){
return;
}
setParamAt( convertExpressionIfNeeded( params[0], params[1] ), 0 );
for(int p=1; p<params.length; p++){
setParamAt( convertExpressionIfNeeded( params[p], left ), p );
}
}
private static String getKeywordFromOperation(int operation){
int token = 0;
for(int i=1; i<1000; i++){
if(getOperationFromToken(i) == operation){
token = i;
break;
}
}
if(operation == NEGATIVE)  token = SQLTokenizer.MINUS;
if(operation == ISNOTNULL) token =  SQLTokenizer.IS;
String keyword = SQLTokenizer.getKeyWord(token);
if(keyword == null) keyword = "" + (char)token;
return keyword;
}
static int getOperationFromToken( int value ){
switch(value){
case SQLTokenizer.PLUS:         return ADD;
case SQLTokenizer.MINUS:        return SUB;
case SQLTokenizer.ASTERISK:     return MUL;
case SQLTokenizer.SLACH:        return DIV;
case SQLTokenizer.PERCENT:      return MOD;
case SQLTokenizer.EQUALS:       return EQUALS;
case SQLTokenizer.GREATER:      return GREATER;
case SQLTokenizer.GREATER_EQU:  return GRE_EQU;
case SQLTokenizer.LESSER:       return LESSER;
case SQLTokenizer.LESSER_EQU:   return LES_EQU;
case SQLTokenizer.UNEQUALS:     return UNEQUALS;
case SQLTokenizer.BETWEEN:      return BETWEEN;
case SQLTokenizer.LIKE:         return LIKE;
case SQLTokenizer.IN:           return IN;
case SQLTokenizer.IS:           return ISNULL;
case SQLTokenizer.OR:           return OR;
case SQLTokenizer.AND:          return AND;
case SQLTokenizer.NOT:          return NOT;
case SQLTokenizer.BIT_OR:       return BIT_OR;
case SQLTokenizer.BIT_AND:      return BIT_AND;
case SQLTokenizer.BIT_XOR:      return BIT_XOR;
case SQLTokenizer.TILDE:        return BIT_NOT;
default:                        return 0;
}
}
static int getDataType(Expression left, Expression right){
int typeLeft  = left.getDataType();
int typeRight = right.getDataType();
return getDataType( typeLeft, typeRight);
}
static int getBestNumberDataType(int paramDataType){
int dataTypeIdx = Utils.indexOf( paramDataType, DatatypeRange);
if(dataTypeIdx >= NVARCHAR_IDX)
return SQLTokenizer.DOUBLE;
if(dataTypeIdx >= INT_IDX)
return SQLTokenizer.INT;
if(dataTypeIdx >= BIGINT_IDX)
return SQLTokenizer.BIGINT;
if(dataTypeIdx >= MONEY_IDX)
return SQLTokenizer.MONEY;
if(dataTypeIdx >= DECIMAL_IDX)
return SQLTokenizer.DECIMAL;
return SQLTokenizer.DOUBLE;
}
static int getDataType(int typeLeft, int typeRight){
if(typeLeft == typeRight) return typeLeft;
int dataTypeIdx = Math.min( Utils.indexOf( typeLeft, DatatypeRange), Utils.indexOf( typeRight, DatatypeRange) );
if(dataTypeIdx < 0) throw new Error("getDataType(): "+typeLeft+", "+typeRight);
return DatatypeRange[ dataTypeIdx ];
}
static final int OR         = 11; 
static final int AND        = 21; 
static final int NOT        = 31; 
static final int BIT_OR     = 41; 
static final int BIT_AND    = 42; 
static final int BIT_XOR    = 43; 
static final int EQUALS     = 51; 
static final int EQUALS_NULL= 52; 
static final int GREATER    = 53; 
static final int GRE_EQU    = 54; 
static final int LESSER     = 55; 
static final int LES_EQU    = 56; 
static final int UNEQUALS   = 57; 
static final int IN         = 61; 
static final int BETWEEN    = 62; 
static final int LIKE       = 63; 
static final int ISNULL     = 64; 
static final int ISNOTNULL  = ISNULL+1; 
static final int ADD        = 71; 
static final int SUB        = 72; 
static final int MUL        = 81; 
static final int DIV        = 82; 
static final int MOD        = 83; 
static final int BIT_NOT    = 91; 
static final int NEGATIVE   =101; 
private static final int[] DatatypeRange = {
SQLTokenizer.TIMESTAMP,
SQLTokenizer.SMALLDATETIME,
SQLTokenizer.DATE,
SQLTokenizer.TIME,
SQLTokenizer.DOUBLE,
SQLTokenizer.FLOAT,
SQLTokenizer.REAL,
SQLTokenizer.DECIMAL,
SQLTokenizer.NUMERIC,
SQLTokenizer.MONEY,
SQLTokenizer.SMALLMONEY,
SQLTokenizer.BIGINT,
SQLTokenizer.INT,
SQLTokenizer.SMALLINT,
SQLTokenizer.TINYINT,
SQLTokenizer.BIT,
SQLTokenizer.BOOLEAN,
SQLTokenizer.LONGNVARCHAR,
SQLTokenizer.UNIQUEIDENTIFIER,
SQLTokenizer.NVARCHAR,
SQLTokenizer.NCHAR,
SQLTokenizer.VARCHAR,
SQLTokenizer.CHAR,
SQLTokenizer.LONGVARCHAR,
SQLTokenizer.CLOB,
SQLTokenizer.VARBINARY,
SQLTokenizer.BINARY,
SQLTokenizer.LONGVARBINARY,
SQLTokenizer.BLOB,
SQLTokenizer.NULL};
private static int NVARCHAR_IDX = Utils.indexOf( SQLTokenizer.NVARCHAR, DatatypeRange);
private static int INT_IDX = Utils.indexOf( SQLTokenizer.INT, DatatypeRange);
private static int BIGINT_IDX = Utils.indexOf( SQLTokenizer.BIGINT, DatatypeRange);
private static int MONEY_IDX = Utils.indexOf( SQLTokenizer.MONEY, DatatypeRange);
private static int DECIMAL_IDX = Utils.indexOf( SQLTokenizer.DECIMAL, DatatypeRange);
}
package smallsql.junit;
import junit.framework.*;
import java.math.BigDecimal;
import java.sql.*;
public class TestJoins extends BasicTestCase {
private TestValue testValue;
private static final String table = "table_joins";
private static final String table2= "table_joins2";
private static final String table3= "table_joins3";
private static final TestValue[] TESTS = new TestValue[]{
a("tinyint"           , new Byte( (byte)3),     new Byte( (byte)4)),
a("byte"              , new Byte( (byte)3),     new Byte( (byte)4)),
a("smallint"          , new Short( (short)3),   new Short( (short)4)),
a("int"               , new Integer(3),         new Integer(4)),
a("bigint"            , new Long(3),            new Long(4)),
a("real"              , new Float(3.45),        new Float(4.56)),
a("float"             , new Float(3.45),        new Float(4.56)),
a("double"            , new Double(3.45),       new Double(4.56)),
a("smallmoney"        , new Float(3.45),        new Float(4.56)),
a("money"             , new Float(3.45),        new Float(4.56)),
a("money"             , new Double(3.45),       new Double(4.56)),
a("numeric(19,2)"     , new BigDecimal("3.45"), new BigDecimal("4.56")),
a("decimal(19,2)"     , new BigDecimal("3.45"), new BigDecimal("4.56")),
a("varnum(28,2)"      , new BigDecimal(3.45),   new BigDecimal(4.56)),
a("number(28,2)"      , new BigDecimal(3.45),   new BigDecimal(4.56)),
a("varchar(100)"      , new String("abc"),      new String("qwert")),
a("nvarchar(100)"     , new String("abc"),      new String("qwert")),
a("varchar2(100)"     , new String("abc"),      new String("qwert")),
a("nvarchar2(100)"    , new String("abc"),      new String("qwert")),
a("character(100)"    , new String("abc"),      new String("qwert")),
a("char(100)"         , new String("abc"),      new String("qwert")),
a("nchar(100)"        , new String("abc"),      new String("qwert")),
a("text"              , new String("abc"),      new String("qwert")),
a("ntext"             , new String("abc"),      new String("qwert")),
a("date"              , new Date(99, 1,1),      new Date(99, 2,2)),
a("time"              , new Time(9, 1,1),       new Time(9, 2,2)),
a("timestamp"         , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
a("datetime"          , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
a("smalldatetime"     , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
a("binary(100)"       , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("varbinary(100)"    , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("raw(100)"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("long raw"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("longvarbinary"     , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("blob"              , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("image"             , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("boolean"           , Boolean.FALSE,          Boolean.TRUE),
a("bit"               , Boolean.FALSE,          Boolean.TRUE),
a("uniqueidentifier"  , "12345678-3445-3445-3445-1234567890ab",      "12345679-3445-3445-3445-1234567890ab"),
};
TestJoins(TestValue testValue){
super(testValue.dataType);
this.testValue = testValue;
}
private void clear() throws SQLException{
Connection con = AllTests.getConnection();
dropTable( con, table );
dropTable( con, table2 );
dropTable( con, table3 );
}
public void tearDown() throws SQLException{
clear();
}
public void setUp() throws Exception{
clear();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table " + table + "(a " + testValue.dataType +" PRIMARY KEY, b " + testValue.dataType + ")");
st.execute("create table " + table2+ "(c " + testValue.dataType +" PRIMARY KEY, d " + testValue.dataType + ")");
st.execute("create table " + table3+ "(c " + testValue.dataType +" PRIMARY KEY, d " + testValue.dataType + ")");
st.close();
con.close();
con = AllTests.getConnection();
PreparedStatement pr = con.prepareStatement("INSERT into " + table + "(a,b) Values(?,?)");
insertValues( pr );
pr.close();
pr = con.prepareStatement("INSERT into " + table2 + " Values(?,?)");
insertValues( pr );
pr.close();
}
private void insertValues(PreparedStatement pr ) throws Exception{
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.large);
pr.execute();
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.small);
pr.execute();
pr.setObject( 1, testValue.large);
pr.setObject( 2, testValue.large);
pr.execute();
pr.setObject( 1, testValue.large);
pr.setObject( 2, testValue.small);
pr.execute();
pr.setObject( 1, null);
pr.setObject( 2, testValue.small);
pr.execute();
pr.setObject( 1, testValue.small);
pr.setObject( 2, null);
pr.execute();
pr.setObject( 1, null);
pr.setObject( 2, null);
pr.execute();
}
public void runTest() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
rs = st.executeQuery("Select * from " + table + " where 1 = 0");
assertFalse( "To many rows", rs.next() );
assertRowCount( 7, "Select * from " + table);
assertRowCount( 49, "Select * from " + table + " t1, " + table2 + " t2");
assertRowCount( 0, "Select * from " + table + ", " + table3);
assertRowCount( 49, "Select * from ("+ table +"), " + table2);
assertRowCount( 49, "Select * from " + table + " Cross Join " + table2);
assertRowCount( 13, "Select * from " + table + " INNER JOIN " + table2 + " ON " + table + ".a = " + table2 + ".c");
assertRowCount( 13, "Select * from " + table + "       JOIN " + table2 + " ON " + table2 + ".c = " + table + ".a");
assertRowCount( 13, "Select * from {oj " + table + " INNER JOIN " + table2 + " ON " + table + ".a = " + table2 + ".c}");
assertRowCount( 13, "Select * from " + table + " AS t1 INNER JOIN " + table2 + " t2 ON t1.a = t2.c");
assertRowCount( 13, "Select * from {oj " + table + " t1 INNER JOIN " + table2 + " t2 ON t1.a = t2.c}");
assertRowCount( 4, "Select * from " + table + " t1 INNER JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
assertRowCount( 4, "Select * from " + table + " t1       JOIN " + table2 + " t2 ON t1.a = t2.c and t2.d=t1.b");
assertRowCount( 7, "Select * from " + table + " t1 LEFT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
assertRowCount( 7, "Select * from " + table + " t1 LEFT       JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
assertRowCount( 15, "Select * from " + table + " t1 LEFT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c");
assertRowCount( 7, "Select * from " + table + " t1 LEFT OUTER JOIN " + table3 + " t2 ON t1.a = t2.c");
assertRowCount( 7, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
assertRowCount( 7, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table2 + " t2 ON false");
assertRowCount( 15, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c");
assertRowCount( 0, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table3 + " t2 ON t1.a = t2.c");
assertRowCount( 14, "Select * from " + table + " t1 FULL OUTER JOIN " + table2 + " t2 ON 1=0");
assertRowCount( 17, "Select * from " + table + " t1 FULL OUTER JOIN " + table2 + " t2 ON t1.a = t2.c");
assertRowCount( 7, "Select * from " + table + " t1 FULL OUTER JOIN " + table3 + " t2 ON t1.a = t2.c");
assertRowCount( 7, "Select * from " + table3 + " t1 FULL OUTER JOIN " + table + " t2 ON t1.c = t2.a");
assertRowCount( 5, "Select * from " + table + " INNER JOIN (SELECT DISTINCT c FROM " + table2 + ") t1 ON " + table + ".a = t1.c");
st.close();
}
public static Test suite() throws Exception{
TestSuite theSuite = new TestSuite("Joins");
for(int i=0; i<TESTS.length; i++){
theSuite.addTest(new TestJoins( TESTS[i] ) );
}
return theSuite;
}
private static TestValue a(String dataType, Object small, Object large){
TestValue value = new TestValue();
value.dataType  = dataType;
value.small     = small;
value.large     = large;
return value;
}
private static class TestValue{
String dataType;
Object small;
Object large;
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
abstract class RowSource {
abstract boolean isScrollable();
abstract void beforeFirst() throws Exception;
boolean isBeforeFirst() throws SQLException{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
boolean isFirst() throws SQLException{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
abstract boolean first() throws Exception;
boolean previous() throws Exception{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
abstract boolean next() throws Exception;
boolean last() throws Exception{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
boolean isLast() throws Exception{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
boolean isAfterLast() throws SQLException, Exception{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
abstract void afterLast() throws Exception;
boolean absolute(int row) throws Exception{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
boolean relative(int rows) throws Exception{
throw SmallSQLException.create(Language.RSET_FWDONLY);
}
abstract int getRow() throws Exception;
abstract long getRowPosition();
abstract void setRowPosition(long rowPosition) throws Exception;
abstract void nullRow();
abstract void noRow();
abstract boolean rowInserted();
abstract boolean rowDeleted();
boolean hasAlias(){
return true;
}
void setAlias(String name) throws SQLException{
throw SmallSQLException.create(Language.ALIAS_UNSUPPORTED);
}
abstract void execute() throws Exception;
abstract boolean isExpressionsFromThisRowSource(Expressions columns);
}
package smallsql.database;
public class ExpressionFunctionUCase extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.UCASE;
}
final boolean isNull() throws Exception {
return param1.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
return getString().getBytes();
}
final String getString() throws Exception {
if(isNull()) return null;
return param1.getString().toUpperCase();
}
}
package smallsql.database;
abstract class ExpressionFunctionReturnP1 extends ExpressionFunction {
boolean isNull() throws Exception{
return param1.isNull();
}
Object getObject() throws Exception{
if(isNull()) return null;
int dataType = getDataType();
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return getBoolean() ? Boolean.TRUE : Boolean.FALSE;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return getBytes();
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
return new Integer( getInt() );
case SQLTokenizer.BIGINT:
return new Long( getLong() );
case SQLTokenizer.REAL:
return new Float( getFloat() );
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return new Double( getDouble() );
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return Money.createFromUnscaledValue( getMoney() );
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return getNumeric();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return getString();
case SQLTokenizer.LONGVARBINARY:
return getBytes();
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
return new DateTime( getLong(), dataType );
case SQLTokenizer.UNIQUEIDENTIFIER:
return getBytes();
default: throw createUnspportedDataType(param1.getDataType());
}
}
int getDataType() {
return param1.getDataType();
}
int getPrecision() {
return param1.getPrecision();
}
final int getScale(){
return param1.getScale();
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
class MemoryStream {
private byte[] puffer;
private int offset;
MemoryStream(){
puffer = new byte[256];
}
void writeTo(FileChannel file) throws IOException{
ByteBuffer buffer = ByteBuffer.wrap( puffer, 0, offset );
file.write(buffer);
}
void writeByte(int value){
verifyFreePufferSize(1);
puffer[ offset++ ] = (byte)(value);
}
void writeShort(int value){
verifyFreePufferSize(2);
puffer[ offset++ ] = (byte)(value >> 8);
puffer[ offset++ ] = (byte)(value);
}
void writeInt(int value){
verifyFreePufferSize(4);
puffer[ offset++ ] = (byte)(value >> 24);
puffer[ offset++ ] = (byte)(value >> 16);
puffer[ offset++ ] = (byte)(value >> 8);
puffer[ offset++ ] = (byte)(value);
}
void writeLong(long value){
verifyFreePufferSize(8);
puffer[ offset++ ] = (byte)(value >> 56);
puffer[ offset++ ] = (byte)(value >> 48);
puffer[ offset++ ] = (byte)(value >> 40);
puffer[ offset++ ] = (byte)(value >> 32);
puffer[ offset++ ] = (byte)(value >> 24);
puffer[ offset++ ] = (byte)(value >> 16);
puffer[ offset++ ] = (byte)(value >> 8);
puffer[ offset++ ] = (byte)(value);
}
void writeChars(char[] value){
verifyFreePufferSize(2*value.length);
for(int i=0; i<value.length; i++){
char c = value[i];
puffer[ offset++ ] = (byte)(c >> 8);
puffer[ offset++ ] = (byte)(c);
}
}
void writeBytes(byte[] value, int off, int length){
verifyFreePufferSize(length);
System.arraycopy(value, off, puffer, offset, length);
offset += length;
}
private void verifyFreePufferSize(int freeSize){
int minSize = offset+freeSize;
if(minSize < puffer.length){
int newSize = puffer.length << 1;
if(newSize < minSize) newSize = minSize;
byte[] temp = new byte[newSize];
System.arraycopy(puffer, 0, temp, 0, offset);
puffer = temp;
}
}
void skip(int count){
offset += count;
}
int readByte(){
return puffer[ offset++ ];
}
int readShort(){
return ((puffer[ offset++ ] & 0xFF) << 8) | (puffer[ offset++ ] & 0xFF);
}
int readInt(){
return ((puffer[ offset++ ] & 0xFF) << 24)
| ((puffer[ offset++ ] & 0xFF) << 16)
| ((puffer[ offset++ ] & 0xFF) << 8)
|  (puffer[ offset++ ] & 0xFF);
}
long readLong(){
return (((long)(puffer[ offset++ ] & 0xFF)) << 56)
| (((long)(puffer[ offset++ ] & 0xFF)) << 48)
| (((long)(puffer[ offset++ ] & 0xFF)) << 40)
| (((long)(puffer[ offset++ ] & 0xFF)) << 32)
| ((puffer[ offset++ ] & 0xFF) << 24)
| ((puffer[ offset++ ] & 0xFF) << 16)
| ((puffer[ offset++ ] & 0xFF) << 8)
|  (puffer[ offset++ ] & 0xFF);
}
char[] readChars(int length){
char[] chars = new char[length];
for(int i=0; i<length; i++){
chars[i] = (char)readShort();
}
return chars;
}
byte[] readBytes(int length){
byte[] bytes = new byte[length];
System.arraycopy(puffer, offset, bytes, 0, length);
offset += length;
return bytes;
}
}
package smallsql.database;
final class ExpressionFunctionPower extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.POWER; }
boolean isNull() throws Exception{
return param1.isNull() || param2.isNull();
}
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.pow( param1.getDouble(), param2.getDouble() );
}
}
package smallsql.junit;
import junit.framework.*;
import java.sql.*;
import java.util.Properties;
public class AllTests extends TestCase{
final static String CATALOG = "AllTests";
final static String JDBC_URL = "jdbc:smallsql:" + CATALOG;
private static Connection con;
public static Connection getConnection() throws SQLException{
if(con == null || con.isClosed()){
con = createConnection();
}
return con;
}
public static Connection createConnection() throws SQLException{
new smallsql.database.SSDriver();
new sun.jdbc.odbc.JdbcOdbcDriver();
return DriverManager.getConnection(JDBC_URL + "?create=true;locale=en");
}
public static Connection createConnection(String urlAddition,
Properties info)
throws SQLException {
new smallsql.database.SSDriver();
new sun.jdbc.odbc.JdbcOdbcDriver();
if (urlAddition == null) urlAddition = "";
if (info == null) info = new Properties();
String urlComplete = JDBC_URL + urlAddition;
return DriverManager.getConnection(urlComplete, info);
}
public static void printRS( ResultSet rs ) throws SQLException{
while(rs.next()){
for(int i=1; i<=rs.getMetaData().getColumnCount(); i++){
System.out.print(rs.getObject(i)+"\t");
}
System.out.println();
}
}
public static Test suite() throws Exception{
TestSuite theSuite = new TestSuite("SmallSQL all Tests");
theSuite.addTestSuite( TestAlterTable.class );
theSuite.addTestSuite( TestAlterTable2.class );
theSuite.addTest    ( TestDataTypes.suite() );
theSuite.addTestSuite(TestDBMetaData.class);
theSuite.addTestSuite(TestExceptionMethods.class);
theSuite.addTest     (TestExceptions.suite());
theSuite.addTestSuite(TestDeleteUpdate.class);
theSuite.addTest     (TestFunctions.suite() );
theSuite.addTestSuite(TestGroupBy.class);
theSuite.addTestSuite(TestIdentifer.class);
theSuite.addTest     (TestJoins.suite());
theSuite.addTestSuite(TestLanguage.class);
theSuite.addTestSuite(TestMoneyRounding.class );
theSuite.addTest     (TestOperatoren.suite() );
theSuite.addTestSuite(TestOrderBy.class);
theSuite.addTestSuite(TestOther.class);
theSuite.addTestSuite(TestResultSet.class);
theSuite.addTestSuite(TestScrollable.class);
theSuite.addTestSuite(TestStatement.class);
theSuite.addTestSuite(TestThreads.class);
theSuite.addTestSuite(TestTokenizer.class);
theSuite.addTestSuite(TestTransactions.class);
return theSuite;
}
public static void main(String[] argv) {
try{
junit.textui.TestRunner.main(new String[]{AllTests.class.getName()});
}catch(Throwable e){
e.printStackTrace();
}
}
}
package smallsql.database;
final class ExpressionFunctionLog extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.LOG; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.log( param1.getDouble() );
}
}
package smallsql.database;
class CommandUpdate extends CommandSelect {
private Expressions sources = new Expressions();
private Expression[] newRowSources;
CommandUpdate( Logger log ){
super(log);
}
void addSetting(Expression dest, Expression source){
columnExpressions.add(dest);
sources.add(source);
}
void executeImpl(SSConnection con, SSStatement st) throws Exception {
int count = columnExpressions.size();
columnExpressions.addAll(sources);
compile(con);
columnExpressions.setSize(count);
newRowSources = sources.toArray();
updateCount = 0;
from.execute();
for(int i=0; i<columnExpressions.size(); i++){
ExpressionName expr = (ExpressionName)columnExpressions.get(i);
DataSource ds = expr.getDataSource();
TableResult tableResult = (TableResult)ds;
tableResult.lock = SQLTokenizer.UPDATE;
}
while(true){
synchronized(con.getMonitor()){
if(!next()){
return;
}
updateRow(con, newRowSources);
}
updateCount++;
}
}
}
package smallsql.junit;
import java.sql.*;
import java.text.*;
import java.util.Locale;
public class TestDBMetaData extends BasicTestCase {
public TestDBMetaData(){
super();
}
public TestDBMetaData(String arg0) {
super(arg0);
}
public void testGetURL() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
assertEquals( "URL", AllTests.JDBC_URL, md.getURL());
}
public void testVersions() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
assertEquals( "DriverVersion", md.getDriverVersion(), md.getDatabaseProductVersion());
Driver driver = DriverManager.getDriver(AllTests.JDBC_URL);
assertEquals( "MajorVersion", driver.getMajorVersion(), md.getDatabaseMajorVersion());
assertEquals( "MajorVersion", driver.getMajorVersion(), md.getDriverMajorVersion());
assertEquals( "MinorVersion", driver.getMinorVersion(), md.getDatabaseMinorVersion());
assertEquals( "MinorVersion", driver.getMinorVersion(), md.getDriverMinorVersion());
assertEquals( "Version", new DecimalFormat("###0.00", new DecimalFormatSymbols(Locale.US)).format(driver.getMajorVersion()+driver.getMinorVersion()/100.0), md.getDriverVersion());
assertTrue( "jdbcCompliant", driver.jdbcCompliant() );
}
public void testFunctions() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
assertEquals( "getNumericFunctions", "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE",
md.getNumericFunctions());
assertEquals( "getStringFunctions", "ASCII,BIT_LENGTH,CHAR_LENGTH,CHARACTER_LENGTH,CHAR,CONCAT,DIFFERENCE,INSERT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,OCTET_LENGTH,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,TRIM,UCASE",
md.getStringFunctions());
assertEquals( "getStringFunctions", "IFNULL,USER,CONVERT,CAST,IIF",
md.getSystemFunctions());
assertEquals( "getStringFunctions", "CURDATE,CURRENT_DATE,CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,DAY,HOUR,MILLISECOND,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR",
md.getTimeDateFunctions());
}
public void testGetProcedures() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getProcedures( null, null, "*");
String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS", "PROCEDURE_TYPE"};
int[] colTypes = {Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL };
assertRSMetaData( rs, colNames, colTypes);
}
public void testGetProcedureColumns() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getProcedureColumns( null, null, "*", null);
String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS" };
int[] colTypes = {Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL };
assertRSMetaData( rs, colNames, colTypes);
}
public void testGetTables() throws Exception{
String[] colNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS","TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"};
int[] types = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL};
Connection con = DriverManager.getConnection("jdbc:smallsql?");
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getTables(null, null, null, null);
super.assertRSMetaData(rs, colNames, new int[colNames.length]); 
assertFalse(rs.next());
con.close();
con = AllTests.getConnection();
md = con.getMetaData();
rs = md.getTables(null, null, null, null);
super.assertRSMetaData(rs, colNames, types);
}
public void testGetSchemas() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getSchemas();
String[] colNames = {"TABLE_SCHEM"};
int[] colTypes = {Types.NULL};
assertRSMetaData( rs, colNames, colTypes);
assertFalse(rs.next());
}
public void testGetCatalogs() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("drop database test2\n\r\t");
}catch(SQLException e){
con.createStatement().execute("create database test2");
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getCatalogs();
assertRSMetaData( rs, new String[]{"TABLE_CAT"}, new int[]{Types.VARCHAR});
while(rs.next()){
System.out.println( "testCatalogs:"+rs.getObject(1) );
}
}
public void testGetTableTypes() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getTableTypes();
String[] colNames = {"TABLE_TYPE"};
int[] colTypes = {Types.VARCHAR};
assertRSMetaData( rs, colNames, colTypes);
String type = "";
int count = 0;
while(rs.next()){
String type2 = rs.getString("TABLE_TYPE");
assertTrue( type+"-"+type2, type.compareTo(type2)<0);
type = type2;
count++;
}
assertEquals("Table Type Count", 3, count);
}
public void testGetColumn() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"tableColumns");
dropView( con, "viewColumns");
con.createStatement().execute("create table tableColumns(a int default 5)");
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getColumns(null, null, "tableColumns", null);
String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"};
int[] colTypes = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.NULL, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.NULL, Types.VARCHAR, Types.NULL, Types.NULL, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
assertRSMetaData( rs, colNames, colTypes);
assertTrue( "No row", rs.next() );
assertEquals( "a", rs.getObject("COLUMN_NAME") );
assertEquals( "INT", rs.getObject("TYPE_NAME") );
assertEquals( "5", rs.getObject("COLUMN_Def") );
con.createStatement().execute("create view viewColumns as Select * from tableColumns");
rs = md.getColumns(null, null, "viewColumns", null);
assertRSMetaData( rs, colNames, colTypes);
assertTrue( "No row", rs.next() );
assertEquals( "a", rs.getObject("COLUMN_NAME") );
assertEquals( "INT", rs.getObject("TYPE_NAME") );
assertEquals( "5", rs.getObject("COLUMN_Def") );
dropView( con, "viewColumns");
dropTable( con, "tableColumns");
}
public void testGetTypeInfo() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getTypeInfo();
String[] colNames = {"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
int[] colTypes = {Types.VARCHAR, Types.SMALLINT, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN, Types.NULL, Types.INTEGER, Types.INTEGER, Types.NULL, Types.NULL, Types.NULL };
assertRSMetaData(rs, colNames, colTypes);
assertTrue(rs.next());
int lastDataType = rs.getInt("data_type");
while(rs.next()){
int dataType = rs.getInt("data_type");
assertTrue("Wrong sorting order", dataType>=lastDataType );
lastDataType = dataType;
}
}
public void testGetCrossReference() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"tblCross1");
dropTable(con,"tblCross2");
DatabaseMetaData md = con.getMetaData();
Statement st = con.createStatement();
st.execute("Create Table tblCross1(id1 counter primary key, v nvarchar(100))");
st.execute("Create Table tblCross2(id2 int , v nvarchar(100), foreign key (id2) REFERENCES tblCross1(id1))");
String[] colNames = {"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
int[] colTypes = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT };
ResultSet rs = md.getCrossReference(null,null,"tblCross1",null,null,"tblCross2");
assertRSMetaData(rs, colNames, colTypes);
assertTrue(rs.next());
assertFalse(rs.next());
rs = md.getImportedKeys(null,null,"tblCross2");
assertRSMetaData(rs, colNames, colTypes);
assertTrue(rs.next());
assertFalse(rs.next());
rs = md.getExportedKeys(null,null,"tblCross1");
assertRSMetaData(rs, colNames, colTypes);
assertTrue(rs.next());
assertFalse(rs.next());
dropTable(con,"tblCross1");
dropTable(con,"tblCross2");
}
public void testGetBestRowIdentifier() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"tblBestRow1");
DatabaseMetaData md = con.getMetaData();
Statement st = con.createStatement();
st.execute("Create Table tblBestRow1(id1 counter primary key, v nvarchar(100))");
String[] colNames = {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
int[] colTypes = {Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.NULL, Types.SMALLINT, Types.SMALLINT};
ResultSet rs = md.getBestRowIdentifier(null, null, "tblBestRow1", DatabaseMetaData.bestRowSession, true);
assertRSMetaData(rs, colNames, colTypes);
assertTrue(rs.next());
assertEquals("Columnname:", "id1", rs.getString("COLUMN_NAME"));
assertFalse(rs.next());
String[] colNames2 = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
int[] colTypes2 = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR};
rs = md.getPrimaryKeys(null, null, "tblBestRow1");
assertRSMetaData(rs, colNames2, colTypes2);
assertTrue(rs.next());
assertEquals("Columnname:", "id1", rs.getString("COLUMN_NAME"));
assertFalse(rs.next());
String[] colNames3 = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"};
int[] colTypes3 = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.BOOLEAN, Types.NULL, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.NULL, Types.NULL, Types.NULL, Types.NULL};
rs = md.getIndexInfo(null, null, "tblBestRow1", true, true);
assertRSMetaData(rs, colNames3, colTypes3);
assertTrue(rs.next());
assertEquals("Columnname:", "id1", rs.getString("COLUMN_NAME"));
assertFalse(rs.next());
dropTable(con,"tblBestRow1");
}
public void testGetgetUDTs() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
ResultSet rs = md.getUDTs(null, null, null, null);
String[] colNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS"};
int[] colTypes = new int[colNames.length];
assertRSMetaData( rs, colNames, colTypes);
assertFalse(rs.next());
}
public void testGetConnection() throws Exception{
Connection con = AllTests.getConnection();
DatabaseMetaData md = con.getMetaData();
assertEquals(con, md.getConnection());
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
public class SSResultSetMetaData implements ResultSetMetaData {
Expressions columns;
public int getColumnCount() throws SQLException {
return columns.size();
}
public boolean isAutoIncrement(int column) throws SQLException {
return getColumnExpression( column ).isAutoIncrement();
}
public boolean isCaseSensitive(int column) throws SQLException {
return getColumnExpression( column ).isCaseSensitive();
}
public boolean isSearchable(int column) throws SQLException {
int type = getColumnExpression( column ).getType();
return type == Expression.NAME || type == Expression.FUNCTION;
}
public boolean isCurrency(int column) throws SQLException {
switch(getColumnExpression( column ).getDataType()){
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return true;
}
return false;
}
public int isNullable(int column) throws SQLException {
return getColumnExpression( column ).isNullable() ? columnNullable : columnNoNulls;
}
public boolean isSigned(int column) throws SQLException {
return isSignedDataType(getColumnExpression( column ).getDataType());
}
static boolean isSignedDataType(int dataType) {
switch(dataType){
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.MONEY:
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.REAL:
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return true;
}
return false;
}
static boolean isNumberDataType(int dataType) {
return isSignedDataType(dataType) || dataType == SQLTokenizer.TINYINT;
}
static boolean isBinaryDataType(int dataType) {
switch(dataType){
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return true;
}
return false;
}
static int getDisplaySize(int dataType, int precision, int scale){
switch(dataType){
case SQLTokenizer.BIT:
return 1; 
case SQLTokenizer.BOOLEAN:
return 5; 
case SQLTokenizer.TINYINT:
return 3;
case SQLTokenizer.SMALLINT:
return 6;
case SQLTokenizer.INT:
return 10;
case SQLTokenizer.BIGINT:
case SQLTokenizer.MONEY:
return 19;
case SQLTokenizer.REAL:
return 13;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return 17;
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.JAVA_OBJECT:
case SQLTokenizer.BLOB:
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
return Integer.MAX_VALUE;
case SQLTokenizer.NUMERIC:
return precision + (scale>0 ? 2 : 1);
case SQLTokenizer.VARBINARY:
case SQLTokenizer.BINARY:
return 2 + precision*2;
case SQLTokenizer.SMALLDATETIME:
return 21;
default:
return precision;
}
}
static int getDataTypePrecision(int dataType, int defaultValue){
switch(dataType){
case SQLTokenizer.NULL:
return 0;
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return 1;
case SQLTokenizer.TINYINT:
return 3;
case SQLTokenizer.SMALLINT:
return 5;
case SQLTokenizer.INT:
case SQLTokenizer.SMALLMONEY:
return 10;
case SQLTokenizer.BIGINT:
case SQLTokenizer.MONEY:
return 19;
case SQLTokenizer.REAL:
return 7;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return 15;
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
if(defaultValue == -1)
return 0xFFFF;
return defaultValue;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
if(defaultValue == -1)
return 38;
return defaultValue;
case SQLTokenizer.TIMESTAMP:
return 23;
case SQLTokenizer.TIME:
return 8;
case SQLTokenizer.DATE:
return 10;
case SQLTokenizer.SMALLDATETIME:
return 16;
case SQLTokenizer.UNIQUEIDENTIFIER:
return 36;
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARBINARY:
return Integer.MAX_VALUE;
}
if(defaultValue == -1)
throw new Error("Precision:"+SQLTokenizer.getKeyWord(dataType));
return defaultValue;
}
public int getColumnDisplaySize(int column) throws SQLException {
return getColumnExpression( column ).getDisplaySize();
}
public String getColumnLabel(int column) throws SQLException {
return getColumnExpression( column ).getAlias();
}
public String getColumnName(int column) throws SQLException {
return getColumnExpression( column ).getAlias();
}
public String getSchemaName(int column) throws SQLException {
return null;
}
public int getPrecision(int column) throws SQLException {
return getColumnExpression( column ).getPrecision();
}
public int getScale(int column) throws SQLException {
return getColumnExpression( column ).getScale();
}
public String getTableName(int column) throws SQLException {
return getColumnExpression( column ).getTableName();
}
public String getCatalogName(int column) throws SQLException {
return null;
}
public int getColumnType(int column) throws SQLException {
return SQLTokenizer.getSQLDataType(getColumnExpression( column ).getDataType() );
}
public String getColumnTypeName(int column) throws SQLException {
return SQLTokenizer.getKeyWord( getColumnExpression( column ).getDataType() );
}
public boolean isReadOnly(int column) throws SQLException {
return !getColumnExpression( column ).isDefinitelyWritable();
}
public boolean isWritable(int column) throws SQLException {
return getColumnExpression( column ).isDefinitelyWritable();
}
public boolean isDefinitelyWritable(int column) throws SQLException {
return getColumnExpression( column ).isDefinitelyWritable();
}
public String getColumnClassName(int column) throws SQLException {
switch(getColumnType(column)){
case Types.TINYINT:
case Types.SMALLINT:
case Types.INTEGER:
return "java.lang.Integer";
case Types.BIT:
case Types.BOOLEAN:
return "java.lang.Boolean";
case Types.BINARY:
case Types.VARBINARY:
case Types.LONGVARBINARY:
return "[B";
case Types.BLOB:
return "java.sql.Blob";
case Types.BIGINT:
return "java.lang.Long";
case Types.DECIMAL:
case Types.NUMERIC:
return "java.math.BigDecimal";
case Types.REAL:
return "java.lang.Float";
case Types.FLOAT:
case Types.DOUBLE:
return "java.lang.Double";
case Types.DATE:
return "java.sql.Date";
case Types.TIME:
return "java.sql.Time";
case Types.TIMESTAMP:
return "java.sql.Timestamp";
case Types.CHAR:
case Types.VARCHAR:
case Types.LONGVARCHAR:
case -11: 
return "java.lang.String";
case Types.CLOB:
return "java.sql.Clob";
default: return "java.lang.Object";
}
}
final int getColumnIdx( int column ) throws SQLException{
if(column < 1 || column > columns.size())
throw SmallSQLException.create( Language.COL_IDX_OUT_RANGE, String.valueOf(column));
return column-1;
}
final Expression getColumnExpression( int column ) throws SQLException{
return columns.get( getColumnIdx( column ) );
}
}
package smallsql.junit;
import junit.framework.*;
import java.sql.*;
import java.math.*;
public class TestOperatoren extends BasicTestCase {
private TestValue testValue;
private static final String table = "table_functions";
private static final TestValue[] TESTS = new TestValue[]{
a("tinyint"           , new Byte( (byte)3),     new Byte( (byte)4)),
a("byte"              , new Byte( (byte)3),     new Byte( (byte)4)),
a("smallint"          , new Short( (short)3),   new Short( (short)4)),
a("int"               , new Integer(3),         new Integer(4)),
a("bigint"            , new Long(3),            new Long(4)),
a("real"              , new Float(3.45),        new Float(4.56)),
a("float"             , new Float(3.45),        new Float(4.56)),
a("double"            , new Double(3.45),       new Double(4.56)),
a("smallmoney"        , new Float(3.45),        new Float(4.56)),
a("money"             , new Float(3.45),        new Float(4.56)),
a("money"             , new Double(3.45),       new Double(4.56)),
a("numeric(19,2)"     , new BigDecimal("3.45"), new BigDecimal("4.56")),
a("decimal(19,2)"     , new BigDecimal("3.45"), new BigDecimal("4.56")),
a("varnum(28,2)"      , new BigDecimal("2.34"), new BigDecimal("3.45")),
a("number(28,2)"      , new BigDecimal("2.34"), new BigDecimal("3.45")),
a("varchar(100)"      , new String("abc"),      new String("qwert")),
a("varchar(60000)"    , new String(new char[43210]),      new String("qwert")),
a("nvarchar(100)"     , new String("abc"),      new String("qwert")),
a("varchar2(100)"     , new String("abc"),      new String("qwert")),
a("nvarchar2(100)"    , new String("abc"),      new String("qwert")),
a("character(100)"    , new String("abc"),      new String("qwert")),
a("char(100)"         , new String("abc"),      new String("qwert")),
a("nchar(100)"        , new String("abc"),      new String("qwert")),
a("text"              , new String("abc"),      new String("qwert")),
a("ntext"             , new String("abc"),      new String("qwert")),
a("date"              , new Date(99, 1,1),      new Date(99, 2,2)),
a("time"              , new Time(9, 1,1),       new Time(9, 2,2)),
a("timestamp"         , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
a("datetime"          , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
a("smalldatetime"     , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
a("binary(100)"       , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("varbinary(100)"    , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("varbinary(60000)"  , new byte[54321],        new byte[]{12, 45, 2, 56, 89}),
a("raw(100)"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("long raw"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("longvarbinary"     , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("blob"              , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("image"             , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
a("boolean"           , Boolean.FALSE,          Boolean.TRUE),
a("bit"               , Boolean.FALSE,          Boolean.TRUE),
a("uniqueidentifier"  , "12345678-3445-3445-3445-1234567890ab",      "12345679-3445-3445-3445-1234567890ac"),
};
TestOperatoren(TestValue testValue){
super(testValue.dataType);
this.testValue = testValue;
}
public void tearDown(){
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("drop table " + table);
st.close();
}catch(Throwable e){
}
}
public void setUp(){
tearDown();
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table " + table + "(a " + testValue.dataType +", b " + testValue.dataType + ")");
st.close();
PreparedStatement pr = con.prepareStatement("INSERT into " + table + "(a,b) Values(?,?)");
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.large);
pr.execute();
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.small);
pr.execute();
pr.setObject( 1, testValue.large);
pr.setObject( 2, testValue.large);
pr.execute();
pr.setObject( 1, testValue.large);
pr.setObject( 2, testValue.small);
pr.execute();
pr.setObject( 1, null);
pr.setObject( 2, testValue.small);
pr.execute();
pr.setObject( 1, testValue.small);
pr.setObject( 2, null);
pr.execute();
pr.setObject( 1, null);
pr.setObject( 2, null);
pr.execute();
pr.close();
}catch(Throwable e){
e.printStackTrace();
}
}
public void runTest() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
rs = st.executeQuery("Select * from " + table + " where 1 = 0");
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a = b");
assertTrue( "To few rows", rs.next() );
assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
assertTrue( "To few rows", rs.next() );
assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a <= b and b <= a");
assertTrue( "To few rows", rs.next() );
assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
assertTrue( "To few rows", rs.next() );
assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where (a > (b))");
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a >= b");
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where not (a >= b)");
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a < b");
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a < b or a>b");
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a <= b");
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
rs = st.executeQuery("Select * from " + table + " where a <> b");
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
PreparedStatement pr = con.prepareStatement("Select * from " + table + " where a between ? and ?");
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.large);
rs = pr.executeQuery();
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
pr.close();
pr = con.prepareStatement("Select * from " + table + " where a not between ? and ?");
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.large);
rs = pr.executeQuery();
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
pr.close();
pr = con.prepareStatement("Select * from " + table + " where a in(?,?)");
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.large);
rs = pr.executeQuery();
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertTrue( "To few rows", rs.next() );
assertFalse( "To many rows", rs.next() );
pr.close();
pr = con.prepareStatement("Select * from " + table + " where a not in(?,?)");
pr.setObject( 1, testValue.small);
pr.setObject( 2, testValue.large);
rs = pr.executeQuery();
assertTrue( "To few rows", rs.next());
assertTrue( "To few rows", rs.next());
assertFalse( "To many rows", rs.next() );
pr.close();
st.close();
}
public static Test suite() throws Exception{
TestSuite theSuite = new TestSuite("Operatoren");
for(int i=0; i<TESTS.length; i++){
theSuite.addTest(new TestOperatoren( TESTS[i] ) );
}
return theSuite;
}
public static void main(String[] argv) {
junit.swingui.TestRunner.main(new String[]{TestOperatoren.class.getName()});
}
private static TestValue a(String dataType, Object small, Object large){
TestValue value = new TestValue();
value.dataType  = dataType;
value.small     = small;
value.large     = large;
return value;
}
private static class TestValue{
String dataType;
Object small;
Object large;
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class IndexNode {
final private boolean unique;
final private char digit; 
static final private IndexNode[] EMPTY_NODES = new IndexNode[0];
private IndexNode[] nodes = EMPTY_NODES;
private char[] remainderKey;
private Object value;
protected IndexNode(boolean unique, char digit){
this.unique = unique;
this.digit  = digit;
}
protected IndexNode createIndexNode(boolean unique, char digit){
return new IndexNode(unique, digit);
}
final char getDigit(){
return digit;
}
final boolean getUnique(){
return unique;
}
final boolean isEmpty(){
return nodes == EMPTY_NODES && value == null;
}
final void clear(){
nodes = EMPTY_NODES;
value = null;
remainderKey = null;
}
final void clearValue(){
value = null;
}
final Object getValue(){
return value;
}
final IndexNode[] getChildNodes(){
return nodes;
}
final IndexNode getChildNode(char digit){
int pos = findNodePos(digit);
if(pos >=0) return nodes[pos];
return null;
}
final char[] getRemainderValue(){
return remainderKey;
}
final IndexNode addNode(char digit) throws SQLException{
if(remainderKey != null) moveRemainderValue();
int pos = findNodePos( digit );
if(pos == -1){
IndexNode node = createIndexNode(unique, digit);
saveNode( node );
return node;
}else{
return nodes[pos];
}
}
final void removeNode(char digit){
int pos = findNodePos( digit );
if(pos != -1){
int length = nodes.length-1;
IndexNode[] temp = new IndexNode[length];
System.arraycopy(nodes, 0, temp, 0, pos);
System.arraycopy(nodes, pos+1, temp, pos, length-pos);
nodes = temp;
}
}
final void addNode(char digit, long rowOffset) throws SQLException{
IndexNode node = addNode(digit);
if(node.remainderKey != null) node.moveRemainderValue();
node.saveValue(rowOffset);
}
final void saveValue(long rowOffset) throws SQLException{
if(unique){
if(value != null) throw SmallSQLException.create(Language.KEY_DUPLICATE);
value = new Long(rowOffset);
}else{
LongTreeList list = (LongTreeList)value;
if(list == null){
value = list = new LongTreeList();
}
list.add(rowOffset);
}
}
final void addRemainderKey(long rowOffset, long remainderValue, int charCount) throws SQLException{
saveRemainderValue(remainderValue, charCount);
value = (unique) ? (Object)new Long(rowOffset) : new LongTreeList(rowOffset);
}
final void addRemainderKey(long rowOffset, char[] remainderValue, int offset) throws SQLException{
saveRemainderValue(remainderValue, offset);
value = (unique) ? (Object)new Long(rowOffset) : new LongTreeList(rowOffset);
}
final IndexNode addRoot(char digit) throws SQLException{
IndexNode node = addNode(digit);
if(node.remainderKey != null) node.moveRemainderValue();
return node.addRoot();
}
final IndexNode addRootValue(char[] remainderValue, int offset) throws SQLException{
saveRemainderValue(remainderValue, offset);
return addRoot();
}
final IndexNode addRootValue( long remainderValue, int digitCount) throws SQLException{
saveRemainderValue(remainderValue, digitCount);
return addRoot();
}
private final void moveRemainderValue() throws SQLException{
Object rowOffset = value;
char[] puffer = remainderKey;
value = null;
remainderKey = null;
IndexNode newNode = addNode(puffer[0]);
if(puffer.length == 1){
newNode.value  = rowOffset;
}else{
newNode.moveRemainderValueSub( rowOffset, puffer);
}
}
private final void moveRemainderValueSub( Object rowOffset, char[] remainderValue){
int length = remainderValue.length-1;
this.remainderKey = new char[length];
value = rowOffset;
System.arraycopy( remainderValue, 1, this.remainderKey, 0, length);
}
private final void saveRemainderValue(char[] remainderValue, int offset){
int length = remainderValue.length-offset;
this.remainderKey = new char[length];
System.arraycopy( remainderValue, offset, this.remainderKey, 0, length);
}
private final void saveRemainderValue( long remainderValue, int charCount){
this.remainderKey = new char[charCount];
for(int i=charCount-1, d=0; i>=0; i--){
this.remainderKey[d++] = (char)(remainderValue >> (i<<4));
}
}
final IndexNode addRoot() throws SQLException{
IndexNode root = (IndexNode)value;
if(root == null){
value = root = createIndexNode(unique, (char)-1);
}
return root;
}
private final void saveNode(IndexNode node){
int length = nodes.length;
IndexNode[] temp = new IndexNode[length+1];
if(length == 0){
temp[0] = node;
}else{
int pos = findNodeInsertPos( node.digit, 0, length);
System.arraycopy(nodes, 0, temp, 0, pos);
System.arraycopy(nodes, pos, temp, pos+1, length-pos);
temp[pos] = node;
}
nodes = temp;
}
private final int findNodeInsertPos(char digit, int start, int end){
if(start == end) return start;
int mid = start + (end - start)/2;
char nodeDigit = nodes[mid].digit;
if(nodeDigit == digit) return mid;
if(nodeDigit < digit){
return findNodeInsertPos( digit, mid+1, end );
}else{
if(start == mid) return start;
return findNodeInsertPos( digit, start, mid );
}
}
private final int findNodePos(char digit){
return findNodePos(digit, 0, nodes.length);
}
private final int findNodePos(char digit, int start, int end){
if(start == nodes.length) return -1;
int mid = start + (end - start)/2;
char nodeDigit = nodes[mid].digit;
if(nodeDigit == digit) return mid;
if(nodeDigit < digit){
return findNodePos( digit, mid+1, end );
}else{
if(start == mid) return -1;
return findNodePos( digit, start, mid-1 );
}
}
void save(StoreImpl output) throws SQLException{
output.writeShort(digit);
int length = remainderKey == null ? 0 : remainderKey.length;
output.writeInt(length);
if(length>0) output.writeChars(remainderKey);
if(value == null){
output.writeByte(0);
}else
if(value instanceof Long){
output.writeByte(1);
output.writeLong( ((Long)value).longValue() );
}else
if(value instanceof LongTreeList){
output.writeByte(2);
((LongTreeList)value).save(output);
}else
if(value instanceof IndexNode){
output.writeByte(3);
((IndexNode)value).saveRef(output);
}
output.writeShort(nodes.length);
for(int i=0; i<nodes.length; i++){
nodes[i].saveRef( output );
}
}
void saveRef(StoreImpl output) throws SQLException{
}
IndexNode loadRef( long offset ) throws SQLException{
throw new Error();
}
void load(StoreImpl input) throws SQLException{
int length = input.readInt();
remainderKey = (length>0) ? input.readChars(length) : null;
int valueType = input.readByte();
switch(valueType){
case 0:
value = null;
break;
case 1:
value = new Long(input.readLong());
break;
case 2:
value = new LongTreeList(input);
break;
case 3:
value = loadRef( input.readLong());
break;
default:
throw SmallSQLException.create(Language.INDEX_CORRUPT, String.valueOf(valueType));
}
nodes = new IndexNode[input.readShort()];
for(int i=0; i<nodes.length; i++){
nodes[i] = loadRef( input.readLong() );
}
}
}
package smallsql.database;
final class ExpressionFunctionCase extends Expression
ExpressionFunctionCase() {
super(FUNCTION);
}
private final Expressions cases   = new Expressions();
private final Expressions results = new Expressions();
private Expression elseResult = Expression.NULL;
private int dataType = -1;
final void addCase(Expression condition, Expression result){
cases.add(condition);
results.add(result);
}
final void setElseResult(Expression expr){
elseResult = expr;
}
final void setEnd(){
Expression[] params = new Expression[cases.size()*2 + (elseResult!=null ? 1 : 0)];
int i=0;
for(int p=0; p<cases.size(); p++){
params[i++] = cases  .get( p );
params[i++] = results.get( p );
}
if(i<params.length)
params[i] = elseResult;
super.setParams(params);
}
final void setParams( Expression[] params ){
super.setParams(params);
int i = 0;
for(int p=0; p<cases.size(); p++){
cases  .set( p, params[i++]);
results.set( p, params[i++]);
}
if(i<params.length)
elseResult = params[i];
}
void setParamAt( Expression param, int idx){
super.setParamAt( param, idx );
int p = idx / 2;
if(p>=cases.size()){
elseResult = param;
return;
}
if(idx % 2 > 0){
results.set( p, param );
}else{
cases.set( p, param );
}
}
final int getFunction() {
return SQLTokenizer.CASE;
}
final boolean isNull() throws Exception {
return getResult().isNull();
}
final boolean getBoolean() throws Exception {
return getResult().getBoolean();
}
final int getInt() throws Exception {
return getResult().getInt();
}
final long getLong() throws Exception {
return getResult().getLong();
}
final float getFloat() throws Exception {
return getResult().getFloat();
}
final double getDouble() throws Exception {
return getResult().getDouble();
}
final long getMoney() throws Exception {
return getResult().getMoney();
}
final MutableNumeric getNumeric() throws Exception {
return getResult().getNumeric();
}
final Object getObject() throws Exception {
return getResult().getObject();
}
final String getString() throws Exception {
return getResult().getString();
}
final byte[] getBytes() throws Exception{
return getResult().getBytes();
}
final int getDataType() {
if(dataType < 0){
dataType = elseResult.getDataType();
for(int i=0; i<results.size(); i++){
dataType = ExpressionArithmetic.getDataType(dataType, results.get(i).getDataType());
}
}
return dataType;
}
final int getPrecision(){
int precision = 0;
for(int i=results.size()-1; i>=0; i--){
precision = Math.max(precision, results.get(i).getPrecision());
}
return precision;
}
final int getScale(){
int precision = 0;
for(int i=results.size()-1; i>=0; i--){
precision = Math.max(precision, results.get(i).getScale());
}
return precision;
}
final private Expression getResult() throws Exception{
for(int i=0; i<cases.size(); i++){
if(cases.get(i).getBoolean()) return results.get(i);
}
return elseResult;
}
}
package smallsql.database;
final class ExpressionFunctionLog10 extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.LOG10; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.log( param1.getDouble() ) / divisor;
}
private static final double divisor = Math.log(10);
}
package smallsql.database;
final class IndexNodeScrollStatus {
final boolean asc;
final IndexNode[] nodes;
int idx;
final Object nodeValue;
final int level;
IndexNodeScrollStatus(IndexNode node, boolean asc, boolean scroll, int level){
this.nodes = node.getChildNodes();
nodeValue = node.getValue();
this.asc = asc;
this.idx = (asc ^ scroll) ? nodes.length : -2;
this.level = level;
}
void afterLast(){
idx = (asc) ? nodes.length : -2;
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;
abstract class TableView {
static final int MAGIC_TABLE = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'T';
static final int MAGIC_VIEW  = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'V';
static final int TABLE_VIEW_VERSION = 2;
static final int TABLE_VIEW_OLD_VERSION = 1;
final String name;
final Columns columns;
private long timestamp = System.currentTimeMillis();
static final int LOCK_NONE   = 0; 
static final int LOCK_INSERT = 1; 
static final int LOCK_READ   = 2; 
static final int LOCK_WRITE  = 3; 
static final int LOCK_TAB    = 4; 
TableView(String name, Columns columns){
this.name = name;
this.columns = columns;
}
static TableView load(SSConnection con, Database database, String name) throws SQLException{
FileChannel raFile = null;
try{
String fileName = Utils.createTableViewFileName( database, name );
File file = new File( fileName );
if(!file.exists())
throw SmallSQLException.create(Language.TABLE_OR_VIEW_MISSING, name);
raFile = Utils.openRaFile( file, database.isReadOnly() );
ByteBuffer buffer = ByteBuffer.allocate(8);
raFile.read(buffer);
buffer.position(0);
int magic   = buffer.getInt();
int version = buffer.getInt();
switch(magic){
case MAGIC_TABLE:
case MAGIC_VIEW:
break;
default:
throw SmallSQLException.create(Language.TABLE_OR_VIEW_FILE_INVALID, fileName);
}
if(version > TABLE_VIEW_VERSION)
throw SmallSQLException.create(Language.FILE_TOONEW, new Object[] { new Integer(version), fileName });
if(version < TABLE_VIEW_OLD_VERSION)
throw SmallSQLException.create(Language.FILE_TOOOLD, new Object[] { new Integer(version), fileName });
if(magic == MAGIC_TABLE)
return new Table( database, con, name, raFile, raFile.position(), version);
return new View ( con, name, raFile, raFile.position());
}catch(Throwable e){
if(raFile != null)
try{
raFile.close();
}catch(Exception e2){
DriverManager.println(e2.toString());
}
throw SmallSQLException.createFromException(e);
}
}
File getFile(Database database){
return new File( Utils.createTableViewFileName( database, name ) );
}
FileChannel createFile(SSConnection con, Database database) throws Exception{
if( database.isReadOnly() ){
throw SmallSQLException.create(Language.DB_READONLY);
}
File file = getFile( database );
boolean ok = file.createNewFile();
if(!ok) throw SmallSQLException.create(Language.TABLE_EXISTENT, name);
FileChannel raFile = Utils.openRaFile( file, database.isReadOnly() );
con.add(new CreateFile(file, raFile, con, database));
writeMagic(raFile);
return raFile;
}
abstract void writeMagic(FileChannel raFile) throws Exception;
String getName(){
return name;
}
long getTimestamp(){
return timestamp;
}
final int findColumnIdx(String columnName){
for(int i=0; i<columns.size(); i++){
if( columns.get(i).getName().equalsIgnoreCase(columnName) ) return i;
}
return -1;
}
final Column findColumn(String columnName){
for(int i=0; i<columns.size(); i++){
Column column = columns.get(i);
if( column.getName().equalsIgnoreCase(columnName) ) return column;
}
return null;
}
void close() throws Exception{
}
package smallsql.database;
import java.sql.*;
class SSSavepoint implements Savepoint {
private final int id;
private final String name;
long transactionTime;
SSSavepoint(int id, String name, long transactionTime){
this.id = id;
this.name = name;
this.transactionTime = transactionTime;
}
public int getSavepointId(){
return id;
}
public String getSavepointName(){
return name;
}
}
package smallsql.junit;
import java.sql.*;
public class TestResultSet extends BasicTestCase {
private static boolean init;
protected void setUp() throws Exception{
if(init) return;
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
dropTable( con, "ResultSet");
st.execute("Create Table ResultSet (i int identity, c varchar(30))");
st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs = st.executeQuery("Select * From ResultSet");
rs.moveToInsertRow();
rs.insertRow();
rs.moveToInsertRow();
rs.insertRow();
init = true;
}
public void testScrollStates() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs = st.executeQuery("Select * From ResultSet Where 1=0");
assertTrue("isBeforeFirst", rs.isBeforeFirst() );
assertTrue("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
rs.moveToInsertRow();
rs.insertRow();
rs.beforeFirst();
assertTrue("isBeforeFirst", rs.isBeforeFirst() );
assertFalse("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
assertTrue("next", rs.next() );
assertTrue("isFirst", rs.isFirst() );
assertTrue("rowInserted", rs.rowInserted() );
assertEquals("getRow", 1, rs.getRow() );
assertTrue("isLast", rs.isLast() );
assertFalse("next", rs.next() );
assertFalse("isBeforeFirst", rs.isBeforeFirst() );
assertTrue("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
assertTrue("first", rs.first() );
assertEquals("getRow", 1, rs.getRow() );
assertFalse("previous", rs.previous() );
assertEquals("getRow", 0, rs.getRow() );
assertTrue("isBeforeFirst", rs.isBeforeFirst() );
assertFalse("isAfterLast", rs.isAfterLast() );
assertTrue("last", rs.last() );
assertEquals("getRow", 1, rs.getRow() );
assertTrue("isLast", rs.isLast() );
rs.afterLast();
assertFalse("isBeforeFirst", rs.isBeforeFirst() );
assertTrue("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
}
public void testScrollStatesGroupBy() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs = st.executeQuery("Select i,max(c) From ResultSet Group By i HAVING i=1");
assertEquals("getConcurrency",ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
assertTrue("isBeforeFirst", rs.isBeforeFirst() );
assertFalse("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
rs.beforeFirst();
assertTrue("isBeforeFirst", rs.isBeforeFirst() );
assertFalse("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
assertTrue("next", rs.next() );
assertTrue("isFirst", rs.isFirst() );
assertFalse("rowInserted", rs.rowInserted() );
assertEquals("getRow", 1, rs.getRow() );
assertTrue("isLast", rs.isLast() );
assertFalse("next", rs.next() );
assertFalse("isBeforeFirst", rs.isBeforeFirst() );
assertTrue("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
assertTrue("first", rs.first() );
assertEquals("getRow", 1, rs.getRow() );
assertFalse("previous", rs.previous() );
assertEquals("getRow", 0, rs.getRow() );
assertTrue("isBeforeFirst", rs.isBeforeFirst() );
assertFalse("isAfterLast", rs.isAfterLast() );
assertTrue("last", rs.last() );
assertEquals("getRow", 1, rs.getRow() );
assertTrue("isLast", rs.isLast() );
rs.afterLast();
assertFalse("isBeforeFirst", rs.isBeforeFirst() );
assertTrue("isAfterLast", rs.isAfterLast() );
assertEquals("getRow", 0, rs.getRow() );
}
public void testUpdate() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs;
rs = st.executeQuery("Select * From ResultSet");
assertTrue("next", rs.next());
assertEquals("getRow", 1, rs.getRow() );
int id = rs.getInt("i");
rs.updateShort("c", (short)123 );
assertEquals( (short)123, rs.getShort("c") );
assertEquals( id, rs.getInt("i") ); 
rs.updateRow();
assertEquals( (short)123, rs.getShort("c") );
assertFalse( rs.rowUpdated() );  
assertFalse( rs.rowInserted() );
assertFalse( rs.rowDeleted() );
assertEquals("getRow", 1, rs.getRow() );
rs = st.executeQuery("Select * From ResultSet");
assertTrue("next", rs.next());
rs.updateByte("c", (byte)66 );
assertEquals( (byte)66, rs.getByte("c") );
rs.updateRow();
assertEquals( (short)66, rs.getShort("c") );
}
public void testUpdateAndScroll() throws Exception{
final Object value = "UpdateAndScroll";
Object value1;
Object value2;
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs = st.executeQuery("Select * From ResultSet");
assertTrue("start", rs.last());
value1 = rs.getObject("i");
rs.updateObject("c", value, Types.VARCHAR );
assertEquals("getObject", value, rs.getObject("c"));
assertEquals("getObject", value1, rs.getObject("i"));
assertTrue("first", rs.first());
assertNotSame("getObject", value, rs.getObject("c"));
assertTrue("start", rs.first());
rs.updateObject("c", value, Types.VARCHAR );
assertEquals("getObject", value, rs.getObject("c"));
assertTrue("next", rs.next());
assertNotSame("getObject", value, rs.getObject("c"));
assertTrue("start", rs.last());
rs.updateObject("c", value );
assertEquals("getObject", value, rs.getObject("c"));
assertTrue("previous", rs.previous());
assertNotSame("getObject", value, rs.getObject("c"));
assertTrue("start", rs.first());
rs.updateObject("c", value, Types.VARCHAR );
assertEquals("getObject", value, rs.getObject("c"));
assertTrue("last", rs.last());
assertNotSame("getObject", value, rs.getObject("c"));
assertTrue("start", rs.first());
rs.updateObject("c", value, Types.VARCHAR );
assertEquals("getObject", value, rs.getObject("c"));
rs.refreshRow();
assertNotSame("getObject", value, rs.getObject("c"));
assertTrue("start", rs.first());
value1 = rs.getObject("i");
value2 = rs.getObject("c");
rs.updateObject("c", value);
assertEquals("getObject", value, rs.getObject("c"));
rs.moveToInsertRow();
assertNull("new row", rs.getObject("i"));
assertNull("new row", rs.getObject("c"));
rs.updateObject("c", value);
assertEquals("getObject", value, rs.getObject("c"));
rs.moveToCurrentRow();
assertEquals("getObject", value1, rs.getObject("i"));
assertEquals("getObject", value2, rs.getObject("c"));
}
public void testDelete() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs = st.executeQuery("Select * From ResultSet Where i>1");
assertTrue("next", rs.next());
assertFalse( rs.rowDeleted() );
rs.deleteRow();
assertTrue( rs.rowDeleted() );
}
public void testOther() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery("Select * From ResultSet");
assertEquals(st, rs.getStatement());
rs.clearWarnings();
assertNull(rs.getWarnings());
rs.setFetchDirection(ResultSet.FETCH_FORWARD);
assertEquals( rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
rs.setFetchDirection(ResultSet.FETCH_REVERSE);
assertEquals( rs.getFetchDirection(), ResultSet.FETCH_REVERSE);
rs.setFetchSize(123);
assertEquals( rs.getFetchSize(), 123);
}
}
package smallsql.junit;
import java.sql.*;
import java.util.ArrayList;
public class TestOrderBy extends BasicTestCase {
static private boolean init;
private static final String table1 = "table_OrderBy1";
private static final String table2 = "table_OrderBy2";
private static final String table3 = "table_OrderBy3";
static private int valueCount;
public void init(){
if(init) return;
try{
Connection con = AllTests.getConnection();
dropTable( con, table1 );
dropTable( con, table2 );
dropTable( con, table3 );
Statement st = con.createStatement();
st.execute("create table " + table1 + "(v varchar(30), c char(30), nv nvarchar(30),i int, d float, r real, bi bigint, b boolean)");
st.execute("create table " + table2 + "(c2 char(30))");
st.execute("create table " + table3 + "(vc varchar(30), vb varbinary(30))");
st.close();
PreparedStatement pr = con.prepareStatement("INSERT into " + table1 + "(v,c,nv,i,d,r,bi,b) Values(?,?,?,?,?,?,?,?)");
PreparedStatement pr2= con.prepareStatement("INSERT into " + table2 + "(c2) Values(?)");
for(int i=150; i>-10; i--){
pr.setString( 1, String.valueOf(i));
pr.setString( 2, String.valueOf(i));
pr.setString( 3, String.valueOf( (char)i ));
pr.setInt   ( 4, i );
pr.setDouble( 5, i );
pr.setFloat ( 6, i );
pr.setInt   ( 7, i );
pr.setBoolean( 8, i == 0 );
pr.execute();
pr2.setString( 1, String.valueOf(i));
pr2.execute();
valueCount++;
}
pr.setObject( 1, null, Types.VARCHAR);
pr.setObject( 2, null, Types.VARCHAR);
pr.setObject( 3, null, Types.VARCHAR);
pr.setObject( 4, null, Types.VARCHAR);
pr.setObject( 5, null, Types.VARCHAR);
pr.setObject( 6, null, Types.VARCHAR);
pr.setObject( 7, null, Types.VARCHAR);
pr.setObject( 8, null, Types.VARCHAR);
pr.execute();
pr2.setObject( 1, null, Types.VARCHAR);
pr2.execute();
pr2.setString( 1, "");
pr2.execute();
pr.close();
pr = con.prepareStatement("INSERT into " + table3 + "(vc, vb) Values(?,?)");
pr.setString( 1, table3);
pr.setBytes( 2, table3.getBytes());
pr.execute();
pr.setString( 1, "");
pr.setBytes( 2, new byte[0]);
pr.execute();
pr.setString( 1, null);
pr.setBytes( 2, null);
pr.execute();
init = true;
}catch(Throwable e){
e.printStackTrace();
}
}
public void testOrderBy_char() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by c");
assertTrue( rs.next() );
oldValue = rs.getString("c");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("c");
int count = 1;
while(rs.next()){
String newValue = rs.getString("c");
assertTrue( oldValue + "<" + newValue, oldValue.compareTo( newValue ) < 0 );
oldValue = newValue;
count++;
}
rs.close();
assertEquals( valueCount, count );
}
public void testOrderBy_varchar() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v");
assertTrue( rs.next() );
oldValue = rs.getString("v");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("v");
int count = 1;
while(rs.next()){
String newValue = rs.getString("v");
assertTrue( oldValue + "<" + newValue, oldValue.compareTo( newValue ) < 0 );
oldValue = newValue;
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_varchar_asc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v ASC");
assertTrue( rs.next() );
oldValue = rs.getString("v");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("v");
int count = 1;
while(rs.next()){
String newValue = rs.getString("v");
assertTrue( oldValue.compareTo( newValue ) < 0 );
oldValue = newValue;
count++;
}
rs.close();
assertEquals( valueCount, count );
}
public void testOrderBy_varchar_desc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v desc");
assertTrue( rs.next() );
oldValue = rs.getString("v");
int count = 1;
while(oldValue != null && rs.next()){
String newValue = rs.getString("v");
if(newValue != null){
assertTrue( oldValue.compareTo( newValue ) > 0 );
count++;
}
oldValue = newValue;
}
assertNull(oldValue);
assertFalse( rs.next() );
assertEquals( valueCount, count );
}
public void testOrderBy_varchar_DescAsc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v desc, i asc");
assertTrue( rs.next() );
oldValue = rs.getString("v");
int count = 1;
while(oldValue != null && rs.next()){
String newValue = rs.getString("v");
if(newValue != null){
assertTrue( oldValue.compareTo( newValue ) > 0 );
count++;
}
oldValue = newValue;
}
assertNull(oldValue);
assertFalse( rs.next() );
assertEquals( valueCount, count );
}
public void testOrderBy_varchar_GroupBy() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT first(v) cc FROM " + table1 + " Group By i ORDER  by first(V)");
assertTrue( rs.next() );
oldValue = rs.getString("cc");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("cc");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( rs.getString("cc") ) < 0 );
oldValue = rs.getString("cc");
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_varchar_Join() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " t1 Inner join "+table2+" t2 on t1.c=t2.c2  ORDER  by v");
assertTrue( rs.next() );
oldValue = rs.getString("v");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( rs.getString("v") ) < 0 );
oldValue = rs.getString("v");
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_nvarchar() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by nv");
assertTrue( rs.next() );
oldValue = rs.getString("nv");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("nv");
int count = 1;
while(rs.next()){
assertTrue( String.CASE_INSENSITIVE_ORDER.compare( oldValue, rs.getString("nv") ) <= 0 );
oldValue = rs.getString("nv");
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_int() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Integer oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i");
assertTrue( rs.next() );
oldValue = (Integer)rs.getObject("i");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = (Integer)rs.getObject("i");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( (Integer)rs.getObject("i") ) < 0 );
oldValue = (Integer)rs.getObject("i");
count++;
}
assertEquals( valueCount, count );
}
public void test_function() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
int oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by abs(i)");
assertTrue( rs.next() );
assertNull(rs.getObject("i"));
assertTrue( rs.next() );
oldValue = Math.abs( rs.getInt("i") );
int count = 1;
while(rs.next()){
int newValue = Math.abs( rs.getInt("i") );
assertTrue( oldValue <= newValue );
oldValue = newValue;
count++;
}
assertEquals( valueCount, count );
}
public void test_functionAscDesc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
int oldValue;
int oldValue2;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by abs(i) Asc, i desc");
assertTrue( rs.next() );
assertNull(rs.getObject("i"));
assertTrue( rs.next() );
oldValue = Math.abs( rs.getInt("i") );
oldValue2 = rs.getInt("i");
int count = 1;
while(rs.next()){
int newValue2 = rs.getInt("i");
int newValue = Math.abs( newValue2 );
assertTrue( oldValue <= newValue );
if(oldValue == newValue){
assertTrue( oldValue2 > newValue2 );
}
oldValue = newValue;
oldValue2 = newValue2;
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_int_asc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Integer oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i Asc");
assertTrue( rs.next() );
oldValue = (Integer)rs.getObject("i");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = (Integer)rs.getObject("i");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( (Integer)rs.getObject("i") ) < 0 );
oldValue = (Integer)rs.getObject("i");
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_int_desc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Integer oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i Desc");
assertTrue( rs.next() );
oldValue = (Integer)rs.getObject("i");
int count = 1;
while(oldValue != null && rs.next()){
Integer newValue = (Integer)rs.getObject("i");
if(newValue != null){
assertTrue( oldValue.compareTo( newValue ) > 0 );
count++;
}
oldValue = newValue;
}
assertNull(oldValue);
assertFalse( rs.next() );
assertEquals( valueCount, count );
}
public void testOrderBy_double() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Double oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by d");
assertTrue( rs.next() );
oldValue = (Double)rs.getObject("d");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = (Double)rs.getObject("d");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( (Double)rs.getObject("d") ) < 0 );
oldValue = (Double)rs.getObject("d");
count++;
}
assertEquals( valueCount, count );
}
public void testOrderBy_real() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Float oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by r");
assertTrue( rs.next() );
oldValue = (Float)rs.getObject("r");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = (Float)rs.getObject("r");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( (Float)rs.getObject("r") ) < 0 );
oldValue = (Float)rs.getObject("r");
count++;
}
assertEquals( valueCount, count );
}
public void test_bigint() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Long oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by bi");
assertTrue( rs.next() );
oldValue = (Long)rs.getObject("bi");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = (Long)rs.getObject("bi");
int count = 1;
while(rs.next()){
assertTrue( oldValue.compareTo( (Long)rs.getObject("bi") ) < 0 );
oldValue = (Long)rs.getObject("bi");
count++;
}
assertEquals( valueCount, count );
}
public void test_bigint_withDoublicateValues() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
Long oldValue;
rs = st.executeQuery("SELECT bi/2 bi_2 FROM " + table1 + " ORDER  by (bi/2)");
assertTrue( rs.next() );
oldValue = (Long)rs.getObject("bi_2");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = (Long)rs.getObject("bi_2");
int count = 1;
while(rs.next()){
Long newValue = (Long)rs.getObject("bi_2");
assertTrue( oldValue + "<="+newValue, oldValue.compareTo( newValue ) <= 0 );
oldValue = newValue;
count++;
}
assertEquals( valueCount, count );
}
public void test_boolean() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
boolean oldValue;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by b");
assertTrue( rs.next() );
oldValue = rs.getBoolean("b");
assertFalse(oldValue);
assertTrue(rs.wasNull());
assertTrue( rs.next() );
oldValue = rs.getBoolean("b");
assertFalse(oldValue);
assertFalse(rs.wasNull());
int count = 1;
while(!oldValue && rs.next()){
oldValue = rs.getBoolean("b");
assertFalse(rs.wasNull());
count++;
}
while(oldValue && rs.next()){
oldValue = rs.getBoolean("b");
assertFalse(rs.wasNull());
count++;
}
assertFalse(rs.next());
assertEquals( valueCount, count );
}
public void testVarcharEmpty() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
rs = st.executeQuery("SELECT * FROM " + table3 + " ORDER  by vc");
assertTrue( rs.next() );
assertNull( rs.getObject("vc") );
assertTrue( rs.next() );
assertEquals( "", rs.getObject("vc") );
assertTrue( rs.next() );
assertEquals( table3, rs.getObject("vc") );
assertFalse( rs.next() );
}
public void testVarbinaryEmpty() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
rs = st.executeQuery("SELECT * FROM " + table3 + " ORDER  by vb");
assertTrue( rs.next() );
assertNull( rs.getObject("vb") );
assertTrue( rs.next() );
assertEqualsObject( "", new byte[0], rs.getObject("vb"), false );
assertTrue( rs.next() );
assertEqualsObject( "", table3.getBytes(), rs.getObject("vb"), false );
assertFalse( rs.next() );
}
public void test2Columns() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = null;
String oldValue;
rs = st.executeQuery("SELECT * FROM " + table1+","+table2+" ORDER  by v, c2");
assertTrue( rs.next() );
assertNull( rs.getObject("v") );
assertNull( rs.getObject("c2") );
assertTrue( rs.next() );
oldValue = rs.getString("c2");
int count = 1;
while(rs.next() && rs.getString("v") == null){
String newValue = rs.getString("c2");
assertTrue( oldValue.compareTo( newValue ) < 0 );
oldValue = newValue;
count++;
}
assertEquals( valueCount+1, count );
boolean isNext = true;
while(isNext){
String vValue = rs.getString("v");
assertNull( rs.getObject("c2") );
assertTrue( rs.next() );
oldValue = rs.getString("c2");
assertEquals( vValue, rs.getString("v") );
isNext = rs.next();
count = 1;
while(isNext && vValue.equals(rs.getString("v"))){
String newValue = rs.getString("c2");
assertTrue( oldValue.compareTo( newValue ) < 0 );
oldValue = newValue;
count++;
isNext = rs.next();
}
assertEquals( valueCount+1, count );
}
}
public void testOrderBy_Scollable() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs;
int count;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v");
rs.next();
rs.next();
rs.previous(); 
rs.last();
count = 0;
while(rs.previous()) count++;
assertEquals( valueCount, count );
rs.beforeFirst();
count = -1;
while(rs.next()) count++;
assertEquals( valueCount, count );
rs.beforeFirst();
count = -1;
while(rs.next()) count++;
assertEquals( valueCount, count );
}
public void testOrderBy_ScollableDesc() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs;
int count;
rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i desc, d");
rs.next();
rs.next();
rs.previous(); 
rs.last();
count = 0;
while(rs.previous()) count++;
assertEquals( valueCount, count );
rs.beforeFirst();
count = -1;
while(rs.next()) count++;
assertEquals( valueCount, count );
rs.beforeFirst();
count = -1;
while(rs.next()) count++;
assertEquals( valueCount, count );
}
public void testOrderBy_Scollable2() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v");
int colCount = rs.getMetaData().getColumnCount();
ArrayList result = new ArrayList();
while(rs.next()){
Object[] row = new Object[colCount];
for(int i=0; i<colCount; i++){
row[i] = rs.getObject(i+1);
}
result.add(row);
}
int rowCount = result.size();
while(rs.previous()){
Object[] row = (Object[])result.get(--rowCount);
for(int i=0; i<colCount; i++){
assertEquals( "Difference in row:"+rowCount, row[i], rs.getObject(i+1));
}
}
assertEquals( "RowCount different between next and previous:"+rowCount, 0, rowCount);
}
public void testUnion() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
String oldValue;
rs = st.executeQuery("SELECT v, 5 as Const FROM " + table1 + " Union All Select vc, 6 From " + table3 + " ORDER by v");
assertRSMetaData(rs, new String[]{"v", "Const"}, new int[]{Types.VARCHAR, Types.INTEGER});
assertTrue( rs.next() );
oldValue = rs.getString("v");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("v");
assertNull(oldValue);
assertTrue( rs.next() );
oldValue = rs.getString("v");
int count = 3;
while(rs.next()){
String newValue = rs.getString("v");
assertTrue( oldValue.compareTo( newValue ) < 0 );
oldValue = newValue;
count++;
}
assertEquals( valueCount+4, count );
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
public class Identity extends Number implements Mutable{
final private long filePos;
final private FileChannel raFile;
final private byte[] page = new byte[8];
private long value;
public Identity(FileChannel raFile, long filePos) throws IOException{
ByteBuffer buffer = ByteBuffer.wrap(page);
synchronized(raFile){
raFile.position(filePos);
raFile.read(buffer);
}
value = ((long)(page[ 0 ]) << 56) |
((long)(page[ 1 ] & 0xFF) << 48) |
((long)(page[ 2 ] & 0xFF) << 40) |
((long)(page[ 3 ] & 0xFF) << 32) |
((long)(page[ 4 ] & 0xFF) << 24) |
((page[ 5 ] & 0xFF) << 16) |
((page[ 6 ] & 0xFF) << 8) |
((page[ 7 ] & 0xFF));
this.raFile  = raFile;
this.filePos = filePos;
}
private StorePage createStorePage(){
page[ 0 ] = (byte)(value >> 56);
page[ 1 ] = (byte)(value >> 48);
page[ 2 ] = (byte)(value >> 40);
page[ 3 ] = (byte)(value >> 32);
page[ 4 ] = (byte)(value >> 24);
page[ 5 ] = (byte)(value >> 16);
page[ 6 ] = (byte)(value >> 8);
page[ 7 ] = (byte)(value);
return new StorePage( page, 8, raFile, filePos);
}
void createNextValue(SSConnection con) throws SQLException{
value++;
con.add( createStorePage() );
}
void setNextValue(Expression expr) throws Exception{
long newValue = expr.getLong();
if(newValue > value){
value = newValue;
createStorePage().commit();
}
}
@Override
public float floatValue() {
return value;
}
@Override
public double doubleValue() {
return value;
}
@Override
public int intValue() {
return (int)value;
}
@Override
public long longValue() {
return value;
}
@Override
public String toString(){
return String.valueOf(value);
}
public Object getImmutableObject(){
return new Long(value);
}
}
package smallsql.database;
import java.nio.channels.FileChannel;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import smallsql.database.language.Language;
public class SSConnection implements Connection {
private final boolean readonly;
private Database database;
private boolean autoCommit = true;
int isolationLevel = TRANSACTION_READ_COMMITTED; 
private List commitPages = new ArrayList();
SSConnection( SSConnection con ){
readonly = con.readonly;
database = con.database;
metadata = con.metadata;
log      = con.log;
}
Database getDatabase(boolean returnNull) throws SQLException{
testClosedConnection();
if(!returnNull && database == null) throw SmallSQLException.create(Language.DB_NOTCONNECTED);
return database;
}
Object getMonitor(){
return this;
}
public Statement createStatement() throws SQLException {
return new SSStatement(this);
}
public PreparedStatement prepareStatement(String sql) throws SQLException {
return new SSPreparedStatement( this, sql);
}
public CallableStatement prepareCall(String sql) throws SQLException {
return new SSCallableStatement( this, sql);
}
public String nativeSQL(String sql){
return sql;
}
public void setAutoCommit(boolean autoCommit) throws SQLException {
if(log.isLogging()) log.println("AutoCommit:"+autoCommit);
if(this.autoCommit != autoCommit){
commit();
this.autoCommit = autoCommit;
}
}
public boolean getAutoCommit(){
return autoCommit;
}
void add(TransactionStep storePage) throws SQLException{
testClosedConnection();
synchronized(getMonitor()){
commitPages.add(storePage);
}
}
public void commit() throws SQLException {
log.println("Commit");
testClosedConnection();
synchronized(getMonitor()){
try{
int count = commitPages.size();
for(int i=0; i<count; i++){
TransactionStep page = (TransactionStep)commitPages.get(i);
page.commit();
}
for(int i=0; i<count; i++){
TransactionStep page = (TransactionStep)commitPages.get(i);
page.freeLock();
}
commitPages.clear();
transactionTime = System.currentTimeMillis();
}catch(Throwable e){
rollback();
throw SmallSQLException.createFromException(e);
}
}
}
void rollbackFile(FileChannel raFile) throws SQLException{
testClosedConnection();
synchronized(getMonitor()){
for(int i = commitPages.size() - 1; i >= 0; i--){
TransactionStep page = (TransactionStep)commitPages.get(i);
if(page.raFile == raFile){
page.rollback();
page.freeLock();
}
}
}
}
void rollback(int savepoint) throws SQLException{
testClosedConnection();
synchronized(getMonitor()){
for(int i = commitPages.size() - 1; i >= savepoint; i--){
TransactionStep page = (TransactionStep)commitPages.remove(i);
page.rollback();
page.freeLock();
}
}
}
public void rollback() throws SQLException {
log.println("Rollback");
testClosedConnection();
synchronized(getMonitor()){
int count = commitPages.size();
for(int i=0; i<count; i++){
TransactionStep page = (TransactionStep)commitPages.get(i);
page.rollback();
page.freeLock();
}
commitPages.clear();
transactionTime = System.currentTimeMillis();
}
}
public void close() throws SQLException {
rollback();
database = null;
commitPages = null;
Database.closeConnection(this);
}
final void testClosedConnection() throws SQLException{
if(isClosed()) throw SmallSQLException.create(Language.CONNECTION_CLOSED);
}
public boolean isClosed(){
return (commitPages == null);
}
public DatabaseMetaData getMetaData(){
return metadata;
}
public void setReadOnly(boolean readOnly){
}
public boolean isReadOnly(){
return readonly;
}
public void setCatalog(String catalog) throws SQLException {
testClosedConnection();
database = Database.getDatabase(catalog, this, false);
}
public String getCatalog(){
if(database == null)
return "";
return database.getName();
}
public void setTransactionIsolation(int level) throws SQLException {
if(!metadata.supportsTransactionIsolationLevel(level)) {
throw SmallSQLException.create(Language.ISOLATION_UNKNOWN, String.valueOf(level));
}
isolationLevel = level;
}
public int getTransactionIsolation(){
return isolationLevel;
}
public SQLWarning getWarnings(){
return null;
}
public void clearWarnings(){
}
public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
return new SSStatement( this, resultSetType, resultSetConcurrency);
}
public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
return new SSPreparedStatement( this, sql, resultSetType, resultSetConcurrency);
}
public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
return new SSCallableStatement( this, sql, resultSetType, resultSetConcurrency);
}
public Map getTypeMap(){
return null;
}
public void setTypeMap(Map map){
}
public void setHoldability(int holdability){
this.holdability = holdability;
}
public int getHoldability(){
return holdability;
}
int getSavepoint() throws SQLException{
testClosedConnection();
return commitPages.size(); 
}
public Savepoint setSavepoint() throws SQLException {
return new SSSavepoint(getSavepoint(), null, transactionTime);
}
public Savepoint setSavepoint(String name) throws SQLException {
return new SSSavepoint(getSavepoint(), name, transactionTime);
}
public void rollback(Savepoint savepoint) throws SQLException {
if(savepoint instanceof SSSavepoint){
if(((SSSavepoint)savepoint).transactionTime != transactionTime){
throw SmallSQLException.create(Language.SAVEPT_INVALID_TRANS);
}
rollback( savepoint.getSavepointId() );
return;
}
throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, savepoint);
}
public void releaseSavepoint(Savepoint savepoint) throws SQLException {
if(savepoint instanceof SSSavepoint){
((SSSavepoint)savepoint).transactionTime = 0;
return;
}
throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, new Object[] { savepoint });
}
public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
return new SSStatement( this, resultSetType, resultSetConcurrency);
}
public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
return new SSPreparedStatement( this, sql);
}
public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
return new SSCallableStatement( this, sql, resultSetType, resultSetConcurrency);
}
public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
SSPreparedStatement pr = new SSPreparedStatement( this, sql);
pr.setNeedGeneratedKeys(autoGeneratedKeys);
return pr;
}
public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
SSPreparedStatement pr = new SSPreparedStatement( this, sql);
pr.setNeedGeneratedKeys(columnIndexes);
return pr;
}
public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
SSPreparedStatement pr = new SSPreparedStatement( this, sql);
pr.setNeedGeneratedKeys(columnNames);
return pr;
}
}
package smallsql.database;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import smallsql.database.language.Language;
class MemoryResult extends DataSource {
ExpressionValue[] currentRow;
private final Columns columns = new Columns();
private int rowIdx = -1;
private List rowList = new ArrayList(); 
MemoryResult(){
MemoryResult(Object[][] data, int colCount) throws SQLException{
for(int c=0; c<colCount; c++){
Column column = new Column();
column.setDataType(SQLTokenizer.NULL);
columns.add( column );
}
for(int r=0; r<data.length; r++){
Object[] row = data[r];
ExpressionValue[] rowValues = new ExpressionValue[row.length];
addRow(rowValues);
for(int c=0; c<colCount; c++){
ExpressionValue expr = rowValues[c] = new ExpressionValue();
expr.set( row[c], -1);
Column column = columns.get(c);
if(expr.getDataType() != SQLTokenizer.NULL){
column.setDataType(expr.getDataType());
}
if(expr.getPrecision() > column.getPrecision()){
column.setPrecision(expr.getPrecision());
}
}
}
}
final void addRow(ExpressionValue[] row){
rowList.add(row);
}
final Column getColumn(int colIdx){
return columns.get(colIdx);
}
final void addColumn(Column column){
columns.add(column);
}
final boolean isScrollable(){
return true;
}
final void beforeFirst(){
rowIdx = -1;
currentRow = null;
}
final boolean isBeforeFirst(){
return rowIdx < 0 || rowList.size() == 0;
}
final boolean isFirst(){
return rowIdx == 0 && currentRow != null;
}
final boolean first(){
rowIdx = 0;
return move();
}
final boolean previous(){
if(rowIdx-- < 0) rowIdx = -1;
return move();
}
final boolean next(){
rowIdx++;
return move();
}
final boolean last(){
rowIdx = rowList.size() - 1;
return move();
}
final boolean isLast(){
return rowIdx == rowList.size() - 1 && currentRow != null;
}
final boolean isAfterLast(){
return rowIdx >= rowList.size() || rowList.size() == 0;
}
final void afterLast(){
rowIdx = rowList.size();
currentRow = null;
}
final boolean absolute(int row) throws SQLException{
if(row == 0) throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
rowIdx = (row > 0) ?
Math.min( row - 1, rowList.size() ):
Math.max( row +rowList.size(), -1 );
return move();
}
final boolean relative(int rows){
if(rows == 0) return (currentRow != null);
rowIdx = Math.min( Math.max( rowIdx + rows, -1), rowList.size());
return move();
}
final int getRow(){
return currentRow == null ? 0 : rowIdx+1;
}
final long getRowPosition(){
return rowIdx;
}
final void setRowPosition(long rowPosition) throws Exception{
rowIdx = (int)rowPosition;
move();
}
final boolean rowInserted(){
return false;
}
final boolean rowDeleted(){
return false;
}
void nullRow(){
throw new Error();
}
void noRow(){
currentRow = null;
}
final private boolean move(){
if(rowIdx < rowList.size() && rowIdx >= 0){
currentRow = (ExpressionValue[])rowList.get(rowIdx);
return true;
}
currentRow = null;
return false;
}
boolean isNull( int colIdx ) throws Exception{
return get( colIdx ).isNull();
}
boolean getBoolean( int colIdx ) throws Exception{
return get( colIdx ).getBoolean();
}
int getInt( int colIdx ) throws Exception{
return get( colIdx ).getInt();
}
long getLong( int colIdx ) throws Exception{
return get( colIdx ).getLong();
}
float getFloat( int colIdx ) throws Exception{
return get( colIdx ).getFloat();
}
double getDouble( int colIdx ) throws Exception{
return get( colIdx ).getDouble();
}
long getMoney( int colIdx ) throws Exception{
return get( colIdx ).getMoney();
}
MutableNumeric getNumeric( int colIdx ) throws Exception{
return get( colIdx ).getNumeric();
}
Object getObject( int colIdx ) throws Exception{
return get( colIdx ).getObject();
}
String getString( int colIdx ) throws Exception{
return get( colIdx ).getString();
}
byte[] getBytes( int colIdx ) throws Exception{
return get( colIdx ).getBytes();
}
int getDataType( int colIdx ){
return columns.get( colIdx ).getDataType();
}
final TableView getTableView(){
return null;
}
final void deleteRow() throws Exception{
throw SmallSQLException.create(Language.RSET_READONLY);
}
final void updateRow(Expression[] updateValues) throws Exception{
throw SmallSQLException.create(Language.RSET_READONLY);
}
final void insertRow(Expression[] updateValues) throws Exception{
throw SmallSQLException.create(Language.RSET_READONLY);
}
private Expression get(int colIdx) throws Exception{
if(currentRow == null) throw SmallSQLException.create(Language.ROW_NOCURRENT);
return currentRow[ colIdx ];
}
int getRowCount(){
return rowList.size();
}
void execute() throws Exception{
rowList.clear();
}
}
package smallsql.database;
import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.net.URL;
class SSPreparedStatement extends SSStatement implements PreparedStatement {
private ArrayList batches;
private final int top; 
SSPreparedStatement( SSConnection con, String sql ) throws SQLException {
this( con, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
}
SSPreparedStatement( SSConnection con, String sql, int rsType, int rsConcurrency ) throws SQLException {
super( con, rsType, rsConcurrency );
con.log.println(sql);
SQLParser parser = new SQLParser();
cmd = parser.parse( con, sql );
top = cmd.getMaxRows();
}
public ResultSet executeQuery() throws SQLException {
executeImp();
return cmd.getQueryResult();
}
public int executeUpdate() throws SQLException {
executeImp();
return cmd.getUpdateCount();
}
final private void executeImp() throws SQLException {
checkStatement();
cmd.verifyParams();
if(getMaxRows() != 0 && (top == -1 || top > getMaxRows()))
cmd.setMaxRows(getMaxRows());
cmd.execute( con, this);
}
public void setNull(int parameterIndex, int sqlType) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, null, SQLTokenizer.NULL);
}
public void setBoolean(int parameterIndex, boolean x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, x ? Boolean.TRUE : Boolean.FALSE, SQLTokenizer.BOOLEAN);
}
public void setByte(int parameterIndex, byte x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, new Integer(x), SQLTokenizer.TINYINT);
}
public void setShort(int parameterIndex, short x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, new Integer(x), SQLTokenizer.SMALLINT);
}
public void setInt(int parameterIndex, int x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, new Integer(x), SQLTokenizer.INT);
}
public void setLong(int parameterIndex, long x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, new Long(x), SQLTokenizer.BIGINT);
}
public void setFloat(int parameterIndex, float x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, new Float(x), SQLTokenizer.REAL);
}
public void setDouble(int parameterIndex, double x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, new Double(x), SQLTokenizer.DOUBLE);
}
public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, x, SQLTokenizer.DECIMAL);
}
public void setString(int parameterIndex, String x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, x, SQLTokenizer.VARCHAR);
}
public void setBytes(int parameterIndex, byte[] x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, x, SQLTokenizer.BINARY);
}
public void setDate(int parameterIndex, Date x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, DateTime.valueOf(x), SQLTokenizer.DATE);
}
public void setTime(int parameterIndex, Time x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, DateTime.valueOf(x), SQLTokenizer.TIME);
}
public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, DateTime.valueOf(x), SQLTokenizer.TIMESTAMP);
}
public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
checkStatement();
cmd.setParamValue( parameterIndex, x, SQLTokenizer.LONGVARCHAR, length);
}
public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
checkStatement();
throw new java.lang.UnsupportedOperationException("Method setCharacterStream() not yet implemented.");
}
public void setRef(int i, Ref x) throws SQLException {
checkStatement();
throw new java.lang.UnsupportedOperationException("Method setBlob() not yet implemented.");
}
public void setClob(int i, Clob x) throws SQLException {
checkStatement();
throw new java.lang.UnsupportedOperationException("Method setArray() not yet implemented.");
}
public ResultSetMetaData getMetaData() throws SQLException {
checkStatement();
if(cmd instanceof CommandSelect){
try{
((CommandSelect)cmd).compile(con);
SSResultSetMetaData metaData = new SSResultSetMetaData();
metaData.columns = cmd.columnExpressions;
return metaData;
}catch(Exception e){
throw SmallSQLException.createFromException(e);
}
}
return null;
}
public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
checkStatement();
throw new java.lang.UnsupportedOperationException("Method setTime() not yet implemented.");
}
public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
checkStatement();
throw new java.lang.UnsupportedOperationException("Method setNull() not yet implemented.");
}
public void setURL(int parameterIndex, URL x) throws SQLException {
checkStatement();
throw new java.lang.UnsupportedOperationException("Method getParameterMetaData() not yet implemented.");
}
}
package smallsql.database;
import java.sql.SQLException;
import java.util.ArrayList;
class Index{
final IndexNode rootPage;
Index(boolean unique){
rootPage = new IndexNode(unique, (char)-1);
}
Index(IndexNode rootPage){
this.rootPage = rootPage;
}
IndexScrollStatus createScrollStatus(Expressions expressions){
return new IndexScrollStatus(rootPage, expressions);
}
final Object findRows(Expressions expressions, boolean searchNullValues, ArrayList nodeList) throws Exception{
IndexNode page = rootPage;
int count = expressions.size();
for(int i = 0; i < count; i++){
page = findRows(page, expressions.get(i), searchNullValues, nodeList);
if(page == null)
return null;
if(i + 1 == count)
return page.getValue();
else
page = (IndexNode)page.getValue();
}
throw new Error();
}
final Object findRows(Expression[] expressions, boolean searchNullValues, ArrayList nodeList) throws Exception{
IndexNode page = rootPage;
int count = expressions.length;
for(int i = 0; i < count; i++){
page = findRows(page, expressions[i], searchNullValues, nodeList);
if(page == null)
return null;
if(i + 1 == count)
return page.getValue();
else
page = (IndexNode)page.getValue();
}
throw new Error();
}
final private IndexNode findRows(IndexNode page, Expression expr, boolean searchNullValues, ArrayList nodeList) throws Exception{
if(expr.isNull()){
if(!searchNullValues){
return null;
}
page = findNull(page);
}else{
switch(expr.getDataType()){
case SQLTokenizer.REAL:
page = find( page, floatToBinarySortOrder( expr.getFloat()), 2, nodeList );
break;
case SQLTokenizer.DOUBLE:
case SQLTokenizer.FLOAT:
page = find( page, doubleToBinarySortOrder( expr.getDouble()), 4, nodeList );
break;
case SQLTokenizer.TINYINT:
page = find( page, expr.getInt(), 1, nodeList );
break;
case SQLTokenizer.SMALLINT:
page = find( page, shortToBinarySortOrder( expr.getInt()), 1, nodeList );
break;
case SQLTokenizer.INT:
page = find( page, intToBinarySortOrder( expr.getInt()), 2, nodeList );
break;
case SQLTokenizer.BIGINT:
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
page = find( page, longToBinarySortOrder( expr.getLong()), 4, nodeList );
break;
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.CLOB:
page = find( page, stringToBinarySortOrder( expr.getString(), false ), nodeList );
break;
case SQLTokenizer.NCHAR:
case SQLTokenizer.CHAR:
page = find( page, stringToBinarySortOrder( expr.getString(), true ), nodeList );
break;
case SQLTokenizer.VARBINARY:
case SQLTokenizer.BINARY:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
case SQLTokenizer.UNIQUEIDENTIFIER:
page = find( page, bytesToBinarySortOrder( expr.getBytes()), nodeList );
break;
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
page = find( page, expr.getBoolean() ? 2 : 1, 1, nodeList );
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
page = find( page, numericToBinarySortOrder( expr.getNumeric() ), nodeList );
break;
default:
throw new Error(String.valueOf(expr.getDataType()));
}
}
return page;
}
final void addValues( long rowOffset, Expressions expressions ) throws Exception{
IndexNode page = this.rootPage;
int count = expressions.size();
for(int i=0; i<count; i++){
Expression expr = expressions.get(i);
boolean isLastValues = (i == count-1);
if(expr.isNull()){
page = addNull(page, rowOffset, isLastValues);
}else{
switch(expr.getDataType()){
case SQLTokenizer.REAL:
page = add( page, rowOffset, floatToBinarySortOrder( expr.getFloat()), isLastValues, 2 );
break;
case SQLTokenizer.DOUBLE:
case SQLTokenizer.FLOAT:
page = add( page, rowOffset, doubleToBinarySortOrder( expr.getDouble()), isLastValues, 4 );
break;
case SQLTokenizer.TINYINT:
page = add( page, rowOffset, expr.getInt(), isLastValues, 1 );
break;
case SQLTokenizer.SMALLINT:
page = add( page, rowOffset, shortToBinarySortOrder( expr.getInt()), isLastValues, 1 );
break;
case SQLTokenizer.INT:
page = add( page, rowOffset, intToBinarySortOrder( expr.getInt()), isLastValues, 2 );
break;
case SQLTokenizer.BIGINT:
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
page = add( page, rowOffset, longToBinarySortOrder( expr.getLong()), isLastValues, 4 );
break;
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.LONGNVARCHAR:
page = add( page, rowOffset, stringToBinarySortOrder( expr.getString(), false ), isLastValues );
break;
case SQLTokenizer.NCHAR:
case SQLTokenizer.CHAR:
page = add( page, rowOffset, stringToBinarySortOrder( expr.getString(), true ), isLastValues );
break;
case SQLTokenizer.VARBINARY:
case SQLTokenizer.BINARY:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
case SQLTokenizer.UNIQUEIDENTIFIER:
page = add( page, rowOffset, bytesToBinarySortOrder( expr.getBytes()), isLastValues );
break;
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
page = add( page, rowOffset, expr.getBoolean() ? 2 : 1, isLastValues, 1 );
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
page = add( page, rowOffset, numericToBinarySortOrder( expr.getNumeric()), isLastValues );
break;
default:
throw new Error(String.valueOf(expr.getDataType()));
}
}
}
}
final void removeValue( long rowOffset, Expressions expressions ) throws Exception{
ArrayList nodeList = new ArrayList();
Object obj = findRows(expressions, true, nodeList);
if(!rootPage.getUnique()){
LongTreeList list = (LongTreeList)obj;
list.remove(rowOffset);
if(list.getSize() > 0) return;
}
IndexNode node = (IndexNode)nodeList.get(nodeList.size()-1);
node.clearValue();
for(int i = nodeList.size()-2; i >= 0; i--){
if(!node.isEmpty())
break;
IndexNode parent = (IndexNode)nodeList.get(i);
parent.removeNode( node.getDigit() );
node = parent;
}
}
final private IndexNode findNull(IndexNode page){
return page.getChildNode( (char)0 );
}
final private IndexNode addNull(IndexNode page, long rowOffset, boolean isLastValue) throws SQLException{
if(isLastValue){
page.addNode( (char)0, rowOffset );
return null;
}else
return page.addRoot((char)0);
}
final private IndexNode find(IndexNode node, long key, int digitCount, ArrayList nodeList){
for(int i=digitCount-1; i>=0; i--){
char digit = (char)(key >> (i<<4));
node = node.getChildNode(digit);
if(node == null) return null;
if(nodeList != null) nodeList.add(node);
if(equals(node.getRemainderValue(), key, i)){
return node;
}
}
return node;
}
final private IndexNode add(IndexNode node, long rowOffset, long key, boolean isLastValue, int digitCount) throws SQLException{
for(int i=digitCount-1; i>=0; i--){
char digit = (char)(key >> (i<<4));
if(i == 0){
if(isLastValue){
node.addNode( digit, rowOffset );
return null;
}
return node.addRoot(digit);
}
node = node.addNode(digit);
if(node.isEmpty()){
if(isLastValue){
node.addRemainderKey( rowOffset, key, i );
return null;
}
return node.addRootValue( key, i);
}else
if(equals(node.getRemainderValue(), key, i)){
if(isLastValue){
node.saveValue( rowOffset);
return null;
}
return node.addRoot();
}
}
throw new Error();
}
final private IndexNode find(IndexNode node, char[] key, ArrayList nodeList){
int length = key.length;
int i=-1;
while(true){
char digit = (i<0) ? (length == 0 ? (char)1 : 2)
: (key[i]);
node = node.getChildNode(digit);
if(node == null) return null;
if(nodeList != null) nodeList.add(node);
if(++i == length){
return node;
}
if(equals(node.getRemainderValue(), key, i)){
return node;
}
}
}
final private IndexNode add(IndexNode node, long rowOffset, char[] key, boolean isLast) throws SQLException{
int length = key.length;
int i=-1;
while(true){
char digit = (i<0) ? (length == 0 ? (char)1 : 2)
: (key[i]);
if(++i == length){
if(isLast){
node.addNode( digit, rowOffset );
return null;
}
return node.addRoot(digit);
}
node = node.addNode(digit);
if(node.isEmpty()){
if(isLast){
node.addRemainderKey( rowOffset, key, i );
return null;
}
return node.addRootValue( key, i );
}else
if(equals(node.getRemainderValue(), key, i)){
if(isLast){
node.saveValue(rowOffset);
return null;
}
return node.addRoot();
}
}
}
final void clear(){
rootPage.clear();
}
final static private int floatToBinarySortOrder(float value){
int intValue = Float.floatToIntBits(value);
return (intValue<0) ?
~intValue :
intValue ^ 0x80000000;
}
final static private long doubleToBinarySortOrder(double value){
long intValue = Double.doubleToLongBits(value);
return (intValue<0) ?
~intValue :
intValue ^ 0x8000000000000000L;
}
final static private int shortToBinarySortOrder(int value){
return value ^ 0x8000;
}
final static private int intToBinarySortOrder(int value){
return value ^ 0x80000000;
}
final static private long longToBinarySortOrder(long value){
return value ^ 0x8000000000000000L;
}
final static private char[] stringToBinarySortOrder(String value, boolean needTrim){
int length = value.length();
if(needTrim){
while(length > 0 && value.charAt(length-1) == ' ') length--;
}
char[] puffer = new char[length];
for(int i=0; i<length; i++){
puffer[i] = Character.toLowerCase(Character.toUpperCase( value.charAt(i) ));
}
return puffer;
}
final static private char[] bytesToBinarySortOrder(byte[] value){
int length = value.length;
char[] puffer = new char[length];
for(int i=0; i<length; i++){
puffer[i] = (char)(value[i] & 0xFF);
}
return puffer;
}
final static private char[] numericToBinarySortOrder(MutableNumeric numeric){
int[] value = numeric.getInternalValue();
int count = 1;
int i;
for(i=0; i<value.length; i++){
if(value[i] != 0){
count = 2*(value.length - i)+1;
break;
}
}
char[] puffer = new char[count];
puffer[0] = (char)count;
for(int c=1; c<count;){
puffer[c++] = (char)(value[i] >> 16);
puffer[c++] = (char)value[i++];
}
return puffer;
}
private final boolean equals(char[] src1, char[] src2, int offset2){
if(src1 == null) return false;
int length = src1.length;
if(length != src2.length - offset2) return false;
for(int i=0; i<length; i++){
if(src1[i] != src2[i+offset2]) return false;
}
return true;
}
private final boolean equals(char[] src1, long src2, int charCount){
if(src1 == null) return false;
int length = src1.length;
if(length != charCount) return false;
for(int i=0, d = charCount-1; i<length; i++){
if(src1[i] != (char)((src2 >> (d-- << 4)))) return false;
}
return true;
}
}
package smallsql.database;
public class ExpressionFunctionSoundex extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.SOUNDEX;
}
final boolean isNull() throws Exception {
return param1.isNull();
}
final byte[] getBytes() throws Exception{
throw createUnspportedConversion(SQLTokenizer.BINARY);
}
final String getString() throws Exception {
if(isNull()) return null;
String input = param1.getString();
return getString(input);
}
static String getString(String input){
char[] output = new char[4];
int idx = 0;
input = input.toUpperCase();
if(input.length()>0){
output[idx++] = input.charAt(0);
}
char last = '0';
for(int i=1; idx<4 && i<input.length(); i++){
char c = input.charAt(i);
switch(c){
case 'B':
case 'F':
case 'P':
case 'V':
c = '1';
break;
case 'C':
case 'G':
case 'J':
case 'K':
case 'Q':
case 'S':
case 'X':
case 'Z':
c = '2';
break;
case 'D':
case 'T':
c = '3';
break;
case 'L':
c = '4';
break;
case 'M':
case 'N':
c = '5';
break;
case 'R':
c = '6';
break;
default:
c = '0';
break;
}
if(c > '0' && last != c){
output[idx++] = c;
}
last = c;
}
for(; idx<4;){
output[idx++] = '0';
}
return new String(output);
}
int getPrecision(){
return 4;
}
}
package smallsql.database;
final class ExpressionFunctionMod extends ExpressionFunctionReturnInt {
final int getFunction(){ return SQLTokenizer.MOD; }
boolean isNull() throws Exception{
return param1.isNull() || param2.isNull();
}
final int getInt() throws Exception{
if(isNull()) return 0;
return param1.getInt() % param2.getInt();
}
}
package smallsql.database;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.SQLException;
import smallsql.database.language.Language;
class Utils {
static final String MASTER_FILENAME = "smallsql.master";
static final String TABLE_VIEW_EXTENTION = ".sdb";
private static final String LOB_EXTENTION = ".lob";
static final String IDX_EXTENTION = ".idx";
private static final Integer[] integerCache = new Integer[260];
private static final Short[]   shortCache   = new Short[260];
static{
for(int i=-4; i<256; i++){
integerCache[ i+4 ] = new Integer(i);
shortCache  [ i+4 ] = new Short((short)i);
}
}
static String createTableViewFileName(Database database, String name){
return database.getName() + '/' + name + TABLE_VIEW_EXTENTION;
}
static String createLobFileName(Database database, String name){
return database.getName() + '/' + name + LOB_EXTENTION;
}
static String createIdxFileName(Database database, String name){
return database.getName() + '/' + name + IDX_EXTENTION;
}
static boolean like(String value, String pattern){
if(value == null || pattern == null) return false;
if(pattern.length() == 0) return true;
int mIdx = 0;
int sIdx = 0;
boolean range = false;
weiter:
while(pattern.length() > mIdx && value.length() > sIdx) {
char m = Character.toUpperCase(pattern.charAt(mIdx++));
switch(m) {
case '%':
range = true;
break;
case '_':
sIdx++;
break;
default:
if(range) {
for(; sIdx < value.length(); sIdx++) {
if(Character.toUpperCase(value.charAt(sIdx)) == m) break;
}
if(sIdx >= value.length()) return false;
int lastmIdx = mIdx - 1;
sIdx++;
while(pattern.length() > mIdx && value.length() > sIdx) {
m = Character.toUpperCase(pattern.charAt(mIdx++));
if(Character.toUpperCase(value.charAt(sIdx)) != m) {
if(m == '%' || m == '_') {
mIdx--;
break;
}
mIdx = lastmIdx;
continue weiter;
}
sIdx++;
}
range = false;
}else{
if(Character.toUpperCase(value.charAt(sIdx)) != m) return false;
sIdx++;
}
break;
}
}
while(pattern.length() > mIdx) {
if(Character.toUpperCase(pattern.charAt(mIdx++)) != '%') return false;
}
while(value.length() > sIdx && !range) return false;
return true;
}
static int long2int(long value){
if(value > Integer.MAX_VALUE)
return Integer.MAX_VALUE;
if(value < Integer.MIN_VALUE)
return Integer.MIN_VALUE;
return (int)value;
}
static long double2long(double value){
if(value > Long.MAX_VALUE)
return Long.MAX_VALUE;
if(value < Long.MIN_VALUE)
return Long.MIN_VALUE;
return (long)value;
}
static float bytes2float( byte[] bytes ){
return Float.intBitsToFloat( bytes2int( bytes ) );
}
static double bytes2double( byte[] bytes ){
return Double.longBitsToDouble( bytes2long( bytes ) );
}
static long bytes2long( byte[] bytes ){
long result = 0;
int length = Math.min( 8, bytes.length);
for(int i=0; i<length; i++){
result = (result << 8) | (bytes[i] & 0xFF);
}
return result;
}
static int bytes2int( byte[] bytes ){
int result = 0;
int length = Math.min( 4, bytes.length);
for(int i=0; i<length; i++){
result = (result << 8) | (bytes[i] & 0xFF);
}
return result;
}
static byte[] double2bytes( double value ){
return long2bytes(Double.doubleToLongBits(value));
}
static byte[] float2bytes( float value ){
return int2bytes(Float.floatToIntBits(value));
}
static byte[] long2bytes( long value ){
byte[] result = new byte[8];
result[0] = (byte)(value >> 56);
result[1] = (byte)(value >> 48);
result[2] = (byte)(value >> 40);
result[3] = (byte)(value >> 32);
result[4] = (byte)(value >> 24);
result[5] = (byte)(value >> 16);
result[6] = (byte)(value >> 8);
result[7] = (byte)(value);
return result;
}
static int money2int( long value ) {
if (value < Integer.MIN_VALUE) return Integer.MIN_VALUE;
else if (value > Integer.MAX_VALUE) return Integer.MAX_VALUE;
else return (int) value;
}
static byte[] int2bytes( int value ){
byte[] result = new byte[4];
result[0] = (byte)(value >> 24);
result[1] = (byte)(value >> 16);
result[2] = (byte)(value >> 8);
result[3] = (byte)(value);
return result;
}
static String bytes2hex( byte[] bytes ){
StringBuffer buf = new StringBuffer(bytes.length << 1);
for(int i=0; i<bytes.length; i++){
buf.append( digits[ (bytes[i] >> 4) & 0x0F ] );
buf.append( digits[ (bytes[i]     ) & 0x0F ] );
}
return buf.toString();
}
static byte[] hex2bytes( char[] hex, int offset, int length) throws SQLException{
try{
byte[] bytes = new byte[length / 2];
for(int i=0; i<bytes.length; i++){
bytes[i] = (byte)((hexDigit2int( hex[ offset++ ] ) << 4)
| hexDigit2int( hex[ offset++ ] ));
}
return bytes;
}catch(Exception e){
throw SmallSQLException.create(Language.SEQUENCE_HEX_INVALID, String.valueOf(offset)); 
}
}
private static int hexDigit2int(char digit){
if(digit >= '0' && digit <= '9') return digit - '0';
digit |= 0x20;
if(digit >= 'a' && digit <= 'f') return digit - 'W'; 
throw new RuntimeException();
}
static byte[] unique2bytes( String unique ) throws SQLException{
char[] chars = unique.toCharArray();
byte[] daten = new byte[16];
daten[3] = hex2byte( chars, 0 );
daten[2] = hex2byte( chars, 2 );
daten[1] = hex2byte( chars, 4 );
daten[0] = hex2byte( chars, 6 );
daten[5] = hex2byte( chars, 9 );
daten[4] = hex2byte( chars, 11 );
daten[7] = hex2byte( chars, 14 );
daten[6] = hex2byte( chars, 16 );
daten[8] = hex2byte( chars, 19 );
daten[9] = hex2byte( chars, 21 );
daten[10] = hex2byte( chars, 24 );
daten[11] = hex2byte( chars, 26 );
daten[12] = hex2byte( chars, 28 );
daten[13] = hex2byte( chars, 30 );
daten[14] = hex2byte( chars, 32 );
daten[15] = hex2byte( chars, 34 );
return daten;
}
private static byte hex2byte( char[] hex, int offset) throws SQLException{
try{
return (byte)((hexDigit2int( hex[ offset++ ] ) << 4)
| hexDigit2int( hex[ offset++ ] ));
}catch(Exception e){
throw SmallSQLException.create(Language.SEQUENCE_HEX_INVALID_STR, new Object[] { new Integer(offset), new String(hex) });
}
}
static String bytes2unique( byte[] daten, int offset ){
if(daten.length-offset < 16){
byte[] temp = new byte[16];
System.arraycopy(daten, offset, temp, 0, daten.length-offset);
daten = temp;
}
char[] chars = new char[36];
chars[8] = chars[13] = chars[18] = chars[23] = '-';
chars[0] = digits[ (daten[offset+3] >> 4) & 0x0F ];
chars[1] = digits[ (daten[offset+3]     ) & 0x0F ];
chars[2] = digits[ (daten[offset+2] >> 4) & 0x0F ];
chars[3] = digits[ (daten[offset+2]     ) & 0x0F ];
chars[4] = digits[ (daten[offset+1] >> 4) & 0x0F ];
chars[5] = digits[ (daten[offset+1]     ) & 0x0F ];
chars[6] = digits[ (daten[offset+0] >> 4) & 0x0F ];
chars[7] = digits[ (daten[offset+0]     ) & 0x0F ];
chars[ 9] = digits[ (daten[offset+5] >> 4) & 0x0F ];
chars[10] = digits[ (daten[offset+5]     ) & 0x0F ];
chars[11] = digits[ (daten[offset+4] >> 4) & 0x0F ];
chars[12] = digits[ (daten[offset+4]     ) & 0x0F ];
chars[14] = digits[ (daten[offset+7] >> 4) & 0x0F ];
chars[15] = digits[ (daten[offset+7]     ) & 0x0F ];
chars[16] = digits[ (daten[offset+6] >> 4) & 0x0F ];
chars[17] = digits[ (daten[offset+6]     ) & 0x0F ];
chars[19] = digits[ (daten[offset+8] >> 4) & 0x0F ];
chars[20] = digits[ (daten[offset+8]     ) & 0x0F ];
chars[21] = digits[ (daten[offset+9] >> 4) & 0x0F ];
chars[22] = digits[ (daten[offset+9]     ) & 0x0F ];
chars[24] = digits[ (daten[offset+10] >> 4) & 0x0F ];
chars[25] = digits[ (daten[offset+10]     ) & 0x0F ];
chars[26] = digits[ (daten[offset+11] >> 4) & 0x0F ];
chars[27] = digits[ (daten[offset+11]     ) & 0x0F ];
chars[28] = digits[ (daten[offset+12] >> 4) & 0x0F ];
chars[29] = digits[ (daten[offset+12]     ) & 0x0F ];
chars[30] = digits[ (daten[offset+13] >> 4) & 0x0F ];
chars[31] = digits[ (daten[offset+13]     ) & 0x0F ];
chars[32] = digits[ (daten[offset+14] >> 4) & 0x0F ];
chars[33] = digits[ (daten[offset+14]     ) & 0x0F ];
chars[34] = digits[ (daten[offset+15] >> 4) & 0x0F ];
chars[35] = digits[ (daten[offset+15]     ) & 0x0F ];
return new String(chars);
}
static boolean string2boolean( String val){
try{
return Double.parseDouble( val ) != 0;
}catch(NumberFormatException e){
return "true".equalsIgnoreCase( val ) || "yes".equalsIgnoreCase( val ) || "t".equalsIgnoreCase( val );
}
static long doubleToMoney(double value){
if(value < 0)
return (long)(value * 10000 - 0.5);
return (long)(value * 10000 + 0.5);
}
static int indexOf( char value, char[] str, int offset, int length ){
value |= 0x20;
for(int end = offset+length;offset < end; offset++){
if((str[offset] | 0x20) == value) return offset;
}
return -1;
}
static int indexOf( int value, int[] list ){
int offset = 0;
for(int end = list.length; offset < end; offset++){
if((list[offset]) == value) return offset;
}
return -1;
}
static int indexOf( byte[] value, byte[] list, int offset ){
int length = value.length;
loop1:
for(int end = list.length-length; offset <= end; offset++){
for(int i=0; i<length; i++ ){
if(list[offset+i] != value[i]){
continue loop1;
}
}
return offset;
}
return -1;
}
static int compareBytes( byte[] leftBytes, byte[] rightBytes){
int length = Math.min( leftBytes.length, rightBytes.length );
int comp = 0;
for(int i=0; i<length; i++){
if(leftBytes[i] != rightBytes[i]){
comp = leftBytes[i] < rightBytes[i] ? -1 : 1;
break;
}
}
if(comp == 0 && leftBytes.length != rightBytes.length){
comp = leftBytes.length < rightBytes.length ? -1 : 1;
}
return comp;
}
static CommandSelect createMemoryCommandSelect( SSConnection con, String[] colNames, Object[][] data) throws SQLException{
MemoryResult source = new MemoryResult(data, colNames.length);
CommandSelect cmd = new CommandSelect(con.log);
for(int i=0; i<colNames.length; i++){
ExpressionName expr = new ExpressionName(colNames[i]);
cmd.addColumnExpression( expr );
expr.setFrom( source, i, source.getColumn(i));
}
cmd.setSource(source);
return cmd;
}
static final Integer getInteger(int value){
if(value >= -4 && value < 256){
return integerCache[ value+4 ];
}else
return new Integer(value);
}
static final Short getShort(int value){
if(value >= -4 && value < 256){
return shortCache[ value+4 ];
}else
return new Short((short)value);
}
static final FileChannel openRaFile( File file, boolean readonly ) throws FileNotFoundException, SQLException{
RandomAccessFile raFile = new RandomAccessFile(file, readonly ? "r" : "rw" );
FileChannel channel = raFile.getChannel();
if( !readonly ){
try{
FileLock lock = channel.tryLock();
if(lock == null){
throw SmallSQLException.create(Language.CANT_LOCK_FILE, file);
}
}catch(SQLException sqlex){
throw sqlex;
}catch(Throwable th){
throw SmallSQLException.createFromException(Language.CANT_LOCK_FILE, file, th);
}
}
return channel;
}
static final Expressions getExpressionNameFromTree(Expression tree){
Expressions list = new Expressions();
getExpressionNameFromTree( list, tree );
return list;
}
private static final void getExpressionNameFromTree(Expressions list, Expression tree){
if(tree.getType() == Expression.NAME ){
list.add(tree);
}
Expression[] params = tree.getParams();
if(params != null){
for(int i=0; i<params.length; i++){
getExpressionNameFromTree( list, tree );
}
}
}
final static char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
}
package smallsql.database;
final class ExpressionFunctionDayOfMonth extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.DAYOFMONTH;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
DateTime.Details details = new DateTime.Details(param1.getLong());
return details.day;
}
}
package smallsql.database;
final class ExpressionFunctionTan extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.TAN; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.tan( param1.getDouble() );
}
}
package smallsql.database;
class IndexScrollStatus {
private final IndexNode rootPage;
private final Expressions expressions; 
private final java.util.Stack nodeStack = new java.util.Stack(); 
final void reset(){
nodeStack.clear();
boolean asc = (expressions.get(0).getAlias() != SQLTokenizer.DESC_STR);
nodeStack.push( new IndexNodeScrollStatus(rootPage, asc, true, 0) );
}
final long getRowOffset( boolean scroll){
if(longList != null){
long rowOffset = scroll ?
longList.getNext(longListEnum) :
longList.getPrevious(longListEnum);
if(rowOffset < 0){
longList = null;
}else{
return rowOffset;
}
}
while(true){
IndexNodeScrollStatus status = (IndexNodeScrollStatus)nodeStack.peek();
int level = status.level;
if(!status.asc ^ scroll){
int idx = ++status.idx;
if(idx == -1){
if(status.nodeValue != null){
if(status.nodeValue instanceof IndexNode){
level++;
nodeStack.push(
new IndexNodeScrollStatus( 	(IndexNode)status.nodeValue,
(expressions.get(level).getAlias() != SQLTokenizer.DESC_STR),
scroll, level));
continue;
}else
return getReturnValue(status.nodeValue);
}
idx = ++status.idx;
}
if(idx >= status.nodes.length){
if(nodeStack.size() > 1){
nodeStack.pop();
continue;
}else{
status.idx = status.nodes.length; 
return -1;
}
}
IndexNode node = status.nodes[idx];
nodeStack.push( new IndexNodeScrollStatus(node, status.asc, scroll, level) );
}else{
int idx = --status.idx;
if(idx == -1){
if(status.nodeValue != null){
if(status.nodeValue instanceof IndexNode){
level++;
nodeStack.push(
new IndexNodeScrollStatus( 	(IndexNode)status.nodeValue,
(expressions.get(level).getAlias() != SQLTokenizer.DESC_STR),
scroll, level));
continue;
}else
return getReturnValue(status.nodeValue);
}
}
if(idx < 0){
if(nodeStack.size() > 1){
nodeStack.pop();
continue;
}else{
return -1;
}
}
IndexNode node = status.nodes[idx];
nodeStack.push( new IndexNodeScrollStatus(node, status.asc, scroll, level) );
}
}
}
final void afterLast(){
longList = null;
nodeStack.setSize(1);
((IndexNodeScrollStatus)nodeStack.peek()).afterLast();
}
private final long getReturnValue( Object value){
if(rootPage.getUnique()){
return ((Long)value).longValue();
}else{
longList = (LongTreeList)value;
longListEnum.reset();
return longList.getNext(longListEnum); 
}
}
}
package smallsql.database;
final class ExpressionFunctionMonth extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.MONTH;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
DateTime.Details details = new DateTime.Details(param1.getLong());
return details.month+1;
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
class StorePage extends TransactionStep{
byte[] page; 
int pageSize;
long fileOffset; 
StorePage(byte[] page, int pageSize, FileChannel raFile, long fileOffset){
super(raFile);
this.page = page;
this.pageSize = pageSize;
this.fileOffset = fileOffset;
}
final void setPageData(byte[] data, int size){
page = data;
pageSize = size;
}
@Override
long commit() throws SQLException{
try{
if(raFile != null && page != null){
ByteBuffer buffer = ByteBuffer.wrap( page, 0, pageSize );
synchronized(raFile){
if(fileOffset < 0){
fileOffset = raFile.size();
}
raFile.position(fileOffset);
raFile.write(buffer);
}
}
return fileOffset;
}catch(Exception e){
throw SmallSQLException.createFromException(e);
}
}
@Override
final void rollback(){
raFile = null;
}
}
package smallsql.database;
import java.sql.SQLException;
import smallsql.database.language.Language;
abstract class TableViewResult extends DataSource {
SSConnection con;
private String alias;
private long tableTimestamp;
int lock = SQLTokenizer.SELECT;
static TableViewResult createResult(TableView tableView){
if(tableView instanceof Table)
return new TableResult((Table)tableView);
return new ViewResult( (View)tableView );
}
static TableViewResult getTableViewResult(RowSource from) throws SQLException{
if(from instanceof Where){
from = ((Where)from).getFrom();
}
if(from instanceof TableViewResult){
return (TableViewResult)from;
}
throw SmallSQLException.create(Language.ROWSOURCE_READONLY);
}
void setAlias( String alias ){
this.alias = alias;
}
String getAlias(){
return (alias != null) ? alias : getTableView().name;
}
boolean hasAlias(){
return alias != null;
}
boolean init( SSConnection con ) throws Exception{
TableView tableView = getTableView();
if(tableTimestamp != tableView.getTimestamp()){
this.con = con;
tableTimestamp = tableView.getTimestamp();
return true;
}
return false;
}
abstract void deleteRow() throws SQLException;
abstract void updateRow(Expression[] updateValues) throws Exception;
abstract void insertRow(Expression[] updateValues) throws Exception;
final boolean isScrollable(){
return false;
}
}
package smallsql.database;
final class Distinct extends RowSource {
final private Expressions distinctColumns;
final private RowSource rowSource;
private Index index;
private int row;
Distinct(RowSource rowSource, Expressions columns){
this.rowSource = rowSource;
this.distinctColumns = columns;
}
final void execute() throws Exception{
rowSource.execute();
index = new Index(true);
}
final boolean isScrollable() {
return false;
}
final void beforeFirst() throws Exception {
rowSource.beforeFirst();
row = 0;
}
final boolean first() throws Exception {
beforeFirst();
return next();
}
final boolean next() throws Exception {
while(true){
boolean isNext = rowSource.next();
if(!isNext) return false;
Long oldRowOffset = (Long)index.findRows(distinctColumns, true, null);
long newRowOffset = rowSource.getRowPosition();
if(oldRowOffset == null){
index.addValues( newRowOffset, distinctColumns);
row++;
return true;
}else
if(oldRowOffset.longValue() == newRowOffset){
row++;
return true;
}
}
}
final void afterLast() throws Exception {
rowSource.afterLast();
row = 0;
}
final int getRow() throws Exception {
return row;
}
final long getRowPosition() {
return rowSource.getRowPosition();
}
final void setRowPosition(long rowPosition) throws Exception {
rowSource.setRowPosition(rowPosition);
}
final void nullRow() {
rowSource.nullRow();
row = 0;
}
final void noRow() {
rowSource.noRow();
row = 0;
}
final boolean rowInserted(){
return rowSource.rowInserted();
}
final boolean rowDeleted() {
return rowSource.rowDeleted();
}
boolean isExpressionsFromThisRowSource(Expressions columns){
return rowSource.isExpressionsFromThisRowSource(columns);
}
}
package smallsql.database;
import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;
final class Database{
static private HashMap databases = new HashMap();
private final TableViewMap tableViews = new TableViewMap();
private final String name;
private final boolean readonly;
private final File directory;
private final FileChannel master;
private final WeakHashMap connections = new WeakHashMap();
static Database getDatabase(String name, SSConnection con, boolean create) throws SQLException{
if(name == null){
return null;
}
if(name.startsWith("file:")){
name = name.substring(5);
}
File file;
try{
file = new File(name).getCanonicalFile();
}catch(Throwable th){
throw SmallSQLException.createFromException( th );
}
String dbKey = file.getName() + ";readonly=" + con.isReadOnly();
synchronized(databases){
Database db = (Database)databases.get(dbKey);
if(db == null){
if(create && !file.isDirectory()){
CommandCreateDatabase command = new CommandCreateDatabase(con.log, name);
command.execute(con, null);
}
db = new Database( name, file, con.isReadOnly() );
databases.put(dbKey, db);
}
db.connections.put(con, null);
return db;
}
}
private static Database getDatabase(SSConnection con, String name) throws SQLException{
return name == null ?
con.getDatabase(false) :
getDatabase( name, con, false );
}
private Database( String name, File canonicalFile, boolean readonly ) throws SQLException{
try{
this.name = name;
this.readonly = readonly;
directory = canonicalFile;
if(!directory.isDirectory()){
throw SmallSQLException.create(Language.DB_NONEXISTENT, name);
}
File file = new File( directory, Utils.MASTER_FILENAME);
if(!file.exists())
throw SmallSQLException.create(Language.DB_NOT_DIRECTORY, name);
master = Utils.openRaFile( file, readonly );
}catch(Exception e){
throw SmallSQLException.createFromException(e);
}
}
String getName(){
return name;
}
boolean isReadOnly(){
return readonly;
}
static final void closeConnection(SSConnection con) throws SQLException{
synchronized(databases){
Iterator iterator = databases.values().iterator();
while(iterator.hasNext()){
Database database = (Database)iterator.next();
WeakHashMap connections = database.connections;
connections.remove(con);
if(connections.size() == 0){
try {
iterator.remove();
database.close();
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
}
}
}
private final void close() throws Exception{
synchronized(tableViews){
Iterator iterator = tableViews.values().iterator();
while(iterator.hasNext()){
TableView tableView = (TableView)iterator.next();
tableView.close();
iterator.remove();
}
}
master.close();
}
static TableView getTableView(SSConnection con, String catalog, String tableName) throws SQLException{
return getDatabase( con, catalog).getTableView( con, tableName);
}
TableView getTableView(SSConnection con, String tableName) throws SQLException{
synchronized(tableViews){
TableView tableView = tableViews.get(tableName);
if(tableView == null){
tableView = TableView.load(con, this, tableName);
tableViews.put( tableName, tableView);
}
return tableView;
}
}
static void dropTable(SSConnection con, String catalog, String tableName) throws Exception{
getDatabase( con, catalog).dropTable( con, tableName);
}
void dropTable(SSConnection con, String tableName) throws Exception{
synchronized(tableViews){
Table table = (Table)tableViews.get( tableName );
if(table != null){
tableViews.remove( tableName );
table.drop(con);
}else{
Table.drop( this, tableName );
}
}
}
void removeTableView(String tableViewName){
synchronized(tableViews){
tableViews.remove( tableViewName );
}
}
void replaceTable( Table oldTable, Table newTable) throws Exception{
synchronized(tableViews){
tableViews.remove( oldTable.name );
tableViews.remove( newTable.name );
oldTable.close();
newTable.close();
File oldFile = oldTable.getFile(this);
File newFile = newTable.getFile(this);
File tmpFile = new File(Utils.createTableViewFileName( this, "#" + System.currentTimeMillis() + this.hashCode() ));
if( !oldFile.renameTo(tmpFile) ){
throw SmallSQLException.create(Language.TABLE_CANT_RENAME, oldTable.name);
}
if( !newFile.renameTo(oldFile) ){
tmpFile.renameTo(oldFile); 
throw SmallSQLException.create(Language.TABLE_CANT_RENAME, oldTable.name);
}
tmpFile.delete();
}
}
static void dropView(SSConnection con, String catalog, String tableName) throws Exception{
getDatabase( con, catalog).dropView(tableName);
}
void dropView(String viewName) throws Exception{
synchronized(tableViews){
Object view = tableViews.remove( viewName );
if(view != null && !(view instanceof View))
throw SmallSQLException.create(Language.VIEWDROP_NOT_VIEW, viewName);
View.drop( this, viewName );
}
}
private void checkForeignKeys( SSConnection con, ForeignKeys foreignKeys ) throws SQLException{
for(int i=0; i<foreignKeys.size(); i++){
ForeignKey foreignKey = foreignKeys.get(i);
TableView pkTable = getTableView(con, foreignKey.pkTable);
if(!(pkTable instanceof Table)){
throw SmallSQLException.create(Language.FK_NOT_TABLE, foreignKey.pkTable);
}
}
}
void createTable(SSConnection con, String name, Columns columns, IndexDescriptions indexes, ForeignKeys foreignKeys) throws Exception{
checkForeignKeys( con, foreignKeys );
Table table = new Table( this, con, name, columns, indexes, foreignKeys);
synchronized(tableViews){
tableViews.put( name, table);
}
}
Table createTable(SSConnection con, String tableName, Columns columns, IndexDescriptions oldIndexes, IndexDescriptions newIndexes, ForeignKeys foreignKeys) throws Exception{
checkForeignKeys( con, foreignKeys );
Table table = new Table( this, con, tableName, columns, oldIndexes, newIndexes, foreignKeys);
synchronized(tableViews){
tableViews.put( tableName, table);
}
return table;
}
void createView(SSConnection con, String viewName, String sql) throws Exception{
new View( this, con, viewName, sql);
}
static Object[][] getCatalogs(Database database){
List catalogs = new ArrayList();
File baseDir = (database != null) ?
database.directory.getParentFile() :
new File(".");
File dirs[] = baseDir.listFiles();
if(dirs != null)
for(int i=0; i<dirs.length; i++){
if(dirs[i].isDirectory()){
if(new File(dirs[i], Utils.MASTER_FILENAME).exists()){
Object[] catalog = new Object[1];
catalog[0] = dirs[i].getPath();
catalogs.add(catalog);
}
}
}
Object[][] result = new Object[catalogs.size()][];
catalogs.toArray(result);
return result;
}
Strings getTables(String tablePattern){
Strings list = new Strings();
File dirs[] = directory.listFiles();
if(dirs != null)
if(tablePattern == null) tablePattern = "%";
tablePattern += Utils.TABLE_VIEW_EXTENTION;
for(int i=0; i<dirs.length; i++){
String name = dirs[i].getName();
if(Utils.like(name, tablePattern)){
list.add(name.substring( 0, name.length()-Utils.TABLE_VIEW_EXTENTION.length() ));
}
}
return list;
}
Object[][] getColumns( SSConnection con, String tablePattern, String colPattern) throws Exception{
List rows = new ArrayList();
Strings tables = getTables(tablePattern);
for(int i=0; i<tables.size(); i++){
String tableName = tables.get(i);
try{
TableView tab = getTableView( con, tableName);
Columns cols = tab.columns;
for(int c=0; c<cols.size(); c++){
Column col = cols.get(c);
Object[] row = new Object[18];
row[0] = getName(); 			
row[2] = tableName;				
row[3] = col.getName();			
row[4] = Utils.getShort( SQLTokenizer.getSQLDataType( col.getDataType() )); 
row[5] = SQLTokenizer.getKeyWord( col.getDataType() );	
row[6] = Utils.getInteger(col.getColumnSize());
row[8] = Utils.getInteger(col.getScale());
row[9] = Utils.getInteger(10);		
row[10]= Utils.getInteger(col.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls); 
row[12]= col.getDefaultDefinition(); 
row[15]= row[6];				
row[16]= Utils.getInteger(i); 	
row[17]= col.isNullable() ? "YES" : "NO"; 
rows.add(row);
}
}catch(Exception e){
}
}
Object[][] result = new Object[rows.size()][];
rows.toArray(result);
return result;
}
Object[][] getReferenceKeys(SSConnection con, String pkTable, String fkTable) throws SQLException{
List rows = new ArrayList();
Strings tables = (pkTable != null) ? getTables(pkTable) : getTables(fkTable);
for(int t=0; t<tables.size(); t++){
String tableName = tables.get(t);
TableView tab = getTableView( con, tableName);
if(!(tab instanceof Table)) continue;
ForeignKeys references = ((Table)tab).references;
for(int i=0; i<references.size(); i++){
ForeignKey foreignKey = references.get(i);
IndexDescription pk = foreignKey.pk;
IndexDescription fk = foreignKey.fk;
if((pkTable == null || pkTable.equals(foreignKey.pkTable)) &&
(fkTable == null || fkTable.equals(foreignKey.fkTable))){
Strings columnsPk = pk.getColumns();
Strings columnsFk = fk.getColumns();
for(int c=0; c<columnsPk.size(); c++){
Object[] row = new Object[14];
row[0] = getName();				
row[2] = foreignKey.pkTable;	
row[3] = columnsPk.get(c);		
row[4] = getName();				
row[6] = foreignKey.fkTable;	
row[7] = columnsFk.get(c);		
row[8] = Utils.getShort(c+1);	
row[9] = Utils.getShort(foreignKey.updateRule);
row[10]= Utils.getShort(foreignKey.deleteRule); 
row[11]= fk.getName();	
row[12]= pk.getName();	
row[13]= Utils.getShort(DatabaseMetaData.importedKeyNotDeferrable); 
rows.add(row);
}
}
}
}
Object[][] result = new Object[rows.size()][];
rows.toArray(result);
return result;
}
Object[][] getBestRowIdentifier(SSConnection con, String table) throws SQLException{
List rows = new ArrayList();
Strings tables = getTables(table);
for(int t=0; t<tables.size(); t++){
String tableName = tables.get(t);
TableView tab = getTableView( con, tableName);
if(!(tab instanceof Table)) continue;
IndexDescriptions indexes = ((Table)tab).indexes;
for(int i=0; i<indexes.size(); i++){
IndexDescription index = indexes.get(i);
if(index.isUnique()){
Strings columns = index.getColumns();
for(int c=0; c<columns.size(); c++){
String columnName = columns.get(c);
Column column = tab.findColumn(columnName);
Object[] row = new Object[8];
row[0] = Utils.getShort(DatabaseMetaData.bestRowSession);
row[1] = columnName;			
final int dataType = column.getDataType();
row[2] = Utils.getInteger(dataType);
row[3] = SQLTokenizer.getKeyWord(dataType);
row[4] = Utils.getInteger(column.getPrecision());	
row[6] = Utils.getShort(column.getScale());		
row[7] = Utils.getShort(DatabaseMetaData.bestRowNotPseudo);
rows.add(row);
}
}
}
}
Object[][] result = new Object[rows.size()][];
rows.toArray(result);
return result;
}
Object[][] getPrimaryKeys(SSConnection con, String table) throws SQLException{
List rows = new ArrayList();
Strings tables = getTables(table);
for(int t=0; t<tables.size(); t++){
String tableName = tables.get(t);
TableView tab = getTableView( con, tableName);
if(!(tab instanceof Table)) continue;
IndexDescriptions indexes = ((Table)tab).indexes;
for(int i=0; i<indexes.size(); i++){
IndexDescription index = indexes.get(i);
if(index.isPrimary()){
Strings columns = index.getColumns();
for(int c=0; c<columns.size(); c++){
Object[] row = new Object[6];
row[0] = getName(); 			
row[2] = tableName;				
row[3] = columns.get(c);		
row[4] = Utils.getShort(c+1);	
row[5] = index.getName();		
rows.add(row);
}
}
}
}
Object[][] result = new Object[rows.size()][];
rows.toArray(result);
return result;
}
Object[][] getIndexInfo( SSConnection con, String table, boolean unique) throws SQLException {
List rows = new ArrayList();
Strings tables = getTables(table);
Short type = Utils.getShort( DatabaseMetaData.tableIndexOther );
for(int t=0; t<tables.size(); t++){
String tableName = tables.get(t);
TableView tab = getTableView( con, tableName);
if(!(tab instanceof Table)) continue;
IndexDescriptions indexes = ((Table)tab).indexes;
for(int i=0; i<indexes.size(); i++){
IndexDescription index = indexes.get(i);
Strings columns = index.getColumns();
for(int c=0; c<columns.size(); c++){
Object[] row = new Object[13];
row[0] = getName(); 			
row[2] = tableName;				
row[3] = Boolean.valueOf(!index.isUnique());
row[5] = index.getName();		
row[6] = type;					
row[7] = Utils.getShort(c+1);	
row[8] = columns.get(c);		
rows.add(row);
}
}
}
Object[][] result = new Object[rows.size()][];
rows.toArray(result);
return result;
}
}
package smallsql.database;
import java.sql.*;
class TableStorePageInsert extends TableStorePage {
final private StorePageLink link = new StorePageLink();
TableStorePageInsert(SSConnection con, Table table, int lockType){
super( con, table, lockType, -1);
link.page = this;
link.filePos = fileOffset;
}
final long commit() throws SQLException{
long result = super.commit();
link.filePos = fileOffset;
link.page = null;
return result;
}
final StorePageLink getLink(){
return link;
}
}
package smallsql.database;
final class ExpressionFunctionCharLen extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.CHARLEN;
}
boolean isNull() throws Exception {
return param1.isNull();
}
final int getInt() throws Exception {
if(isNull()) return 0;
String str = param1.getString();
return str.length();
}
}
package smallsql.database;
import java.sql.*;
import java.text.DateFormatSymbols;
import java.util.Calendar;
import java.util.TimeZone;
import smallsql.database.language.Language;
public final class DateTime implements Mutable{
long time;
private int dataType = SQLTokenizer.TIMESTAMP;
static final int[] MONTH_DAYS = {0,31,59,90,120,151,181,212,243,273,304,334};
private static final String[] SHORT_MONTHS = new DateFormatSymbols().getShortMonths();
DateTime(long time, int dataType){
switch(dataType){
case SQLTokenizer.SMALLDATETIME:
int seconds = (int)(time % 60000);
if(seconds < 0){
seconds += 60000;
}
time -= seconds;
break;
case SQLTokenizer.TIME:
time %= 86400000;
break;
case SQLTokenizer.DATE:
int millis = (int)(time % 86400000);
if(millis < 0)
millis += 86400000;
time -= millis;
break;
}
this.time = time;
this.dataType = dataType;
}
static long calcMillis(Details details){
return calcMillis(details.year, details.month, details.day, details.hour, details.minute, details.second, details.millis);
}
static long calcMillis(int year, int month, final int day, final int hour, final int minute, final int second, final int millis){
long result = millis;
result += second * 1000;
result += minute * 60000;
result += hour * 3600000;
result += (day-1) * 86400000L;
if(month > 11){
year += month / 12;
month %= 12;
}
result += MONTH_DAYS[month] * 86400000L;
result += (year - 1970) * 31536000000L; 
result += ((year/4) - (year/100) + (year/400) - 477) * 86400000L;
if(month<2 && year % 4 == 0 && (year%100 != 0 || year%400 == 0))
result -= 86400000L;
return result;
}
static long now(){
return removeDateTimeOffset( System.currentTimeMillis() );
}
static int dayOfWeek(long time){
return (int)((time / 86400000 + 3) % 7);
}
static long parse(java.util.Date date){
long t = date.getTime();
return removeDateTimeOffset(t);
}
static DateTime valueOf(java.util.Date date){
if(date == null) return null;
int type;
if(date instanceof java.sql.Date)
type = SQLTokenizer.DATE;
else
if(date instanceof java.sql.Time)
type = SQLTokenizer.TIME;
else
type = SQLTokenizer.TIMESTAMP;
return new DateTime( parse(date), type);
}
static DateTime valueOf(java.sql.Date date){
if(date == null) return null;
return new DateTime( parse(date), SQLTokenizer.DATE);
}
static DateTime valueOf(java.sql.Time date){
if(date == null) return null;
return new DateTime( parse(date), SQLTokenizer.TIME);
}
static DateTime valueOf(java.sql.Timestamp date){
if(date == null) return null;
return new DateTime( parse(date), SQLTokenizer.TIMESTAMP);
}
static DateTime valueOf(String date, int dataType) throws SQLException{
if(date == null) return null;
return new DateTime( parse(date), dataType);
}
static long parse(final String datetime) throws SQLException{
try{
final int length = datetime.length();
final int year;
final int month;
final int day;
final int hour;
final int minute;
final int second;
final int millis;
int idx1 = 0;
int idx2 = datetime.indexOf('-');
if(idx2 > 0){
year = Integer.parseInt(datetime.substring(idx1, idx2).trim());
idx1 = idx2+1;
idx2 = datetime.indexOf('-', idx1);
month = Integer.parseInt(datetime.substring(idx1, idx2).trim())-1;
idx1 = idx2+1;
idx2 = datetime.indexOf(' ', idx1);
if(idx2 < 0) idx2 = datetime.length();
day = Integer.parseInt(datetime.substring(idx1, idx2).trim());
}else{
year  = 1970;
month = 0;
day   = 1;
}
idx1 = idx2+1;
idx2 = datetime.indexOf(':', idx1);
if(idx2>0){
hour = Integer.parseInt(datetime.substring(idx1, idx2).trim());
idx1 = idx2+1;
idx2 = datetime.indexOf(':', idx1);
minute = Integer.parseInt(datetime.substring(idx1, idx2).trim());
idx1 = idx2+1;
idx2 = datetime.indexOf('.', idx1);
if(idx2 < 0) idx2 = datetime.length();
second = Integer.parseInt(datetime.substring(idx1, idx2).trim());
idx1 = idx2+1;
if(idx1 < length){
String strMillis = datetime.substring(idx1).trim();
switch(strMillis.length()){
case 1:
millis = Integer.parseInt(strMillis) * 100;
break;
case 2:
millis = Integer.parseInt(strMillis) * 10;
break;
case 3:
millis = Integer.parseInt(strMillis);
break;
default:
millis = Integer.parseInt(strMillis.substring(0,3));
}
}else
millis = 0;
}else{
hour   = 0;
minute = 0;
second = 0;
millis = 0;
}
if(idx1 == 0 && length > 0){
throw SmallSQLException.create(Language.DATETIME_INVALID);
}
if(month >= 12){
throw SmallSQLException.create(Language.MONTH_TOOLARGE, datetime );
}
if(day >= 32){
throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
}
if(day == 31){
switch(month){
case 1:
case 3:
case 5:
case 8:
case 10:
throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
}
}
if(month == 1){
if(day == 30){
throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
}
if(day == 29){
if(!isLeapYear(year)){
throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
}
}
}
if(hour >= 24){
throw SmallSQLException.create(Language.HOURS_TOOLARGE, datetime );
}
if(minute >= 60){
throw SmallSQLException.create(Language.MINUTES_TOOLARGE, datetime );
}
if(second >= 60){
throw SmallSQLException.create(Language.SECS_TOOLARGE, datetime );
}
if(millis >= 1000){
throw SmallSQLException.create(Language.MILLIS_TOOLARGE, datetime );
}
return calcMillis(year, month, day, hour, minute, second, millis);
}catch(SQLException ex){
throw ex;
}catch(Throwable ex){
throw SmallSQLException.createFromException(Language.DATETIME_INVALID, datetime, ex );
}
}
long getTimeMillis(){
return time;
}
int getDataType(){
return dataType;
}
public String toString(){
Details details = new Details(time);
StringBuffer buf = new StringBuffer();
if(dataType != SQLTokenizer.TIME){
formatNumber( details.year,  4, buf );
buf.append('-');
formatNumber( details.month + 1, 2, buf );
buf.append('-');
formatNumber( details.day,   2, buf );
}
if(dataType != SQLTokenizer.DATE){
if(buf.length() > 0) buf.append(' ');
formatNumber( details.hour,  2, buf );
buf.append(':');
formatNumber( details.minute, 2, buf );
buf.append(':');
formatNumber( details.second, 2, buf );
}
switch(dataType){
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
buf.append('.');
formatMillis( details.millis, buf );
}
return buf.toString();
}
public boolean equals(Object obj){
if(!(obj instanceof DateTime)) return false;
DateTime value = (DateTime)obj;
return value.time == time && value.dataType == dataType;
}
String toString(int style){
if(style < 0)
return toString();
Details details = new Details(time);
StringBuffer buf = new StringBuffer();
switch(style){
case 0:
case 100: 
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.day, 2, buf);
buf.append(' ');
formatNumber( details.year, 4, buf);
buf.append(' ');
formatHour12( details.hour, buf );
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append( details.hour < 12 ? "AM" : "PM" );
return buf.toString();
case 1:   
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.day, 2, buf);
buf.append('/');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 101:   
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.day, 2, buf);
buf.append('/');
formatNumber( details.year, 4, buf);
return buf.toString();
case 2: 
formatNumber( details.year % 100, 2, buf);
buf.append('.');
formatNumber( details.month+1, 2, buf);
buf.append('.');
formatNumber( details.day, 2, buf);
return buf.toString();
case 102: 
formatNumber( details.year, 4, buf);
buf.append('.');
formatNumber( details.month+1, 2, buf);
buf.append('.');
formatNumber( details.day, 2, buf);
return buf.toString();
case 3: 
formatNumber( details.day, 2, buf);
buf.append('/');
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 103: 
formatNumber( details.day, 2, buf);
buf.append('/');
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.year, 4, buf);
return buf.toString();
case 4: 
formatNumber( details.day, 2, buf);
buf.append('.');
formatNumber( details.month+1, 2, buf);
buf.append('.');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 104: 
formatNumber( details.day, 2, buf);
buf.append('.');
formatNumber( details.month+1, 2, buf);
buf.append('.');
formatNumber( details.year, 4, buf);
return buf.toString();
case 5: 
formatNumber( details.day, 2, buf);
buf.append('-');
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 105: 
formatNumber( details.day, 2, buf);
buf.append('-');
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.year, 4, buf);
return buf.toString();
case 6: 
formatNumber( details.day, 2, buf);
buf.append(' ');
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 106: 
formatNumber( details.day, 2, buf);
buf.append(' ');
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.year, 4, buf);
return buf.toString();
case 7: 
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.day, 2, buf);
buf.append(',');
buf.append(' ');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 107: 
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.day, 2, buf);
buf.append(',');
buf.append(' ');
formatNumber( details.year, 4, buf);
return buf.toString();
case 8: 
case 108:
formatNumber( details.hour, 2, buf);
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
return buf.toString();
case 9:
case 109: 
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.day, 2, buf);
buf.append(' ');
formatNumber( details.year, 4, buf);
buf.append(' ');
formatHour12( details.hour, buf );
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append(':');
formatMillis( details.millis, buf);
buf.append( details.hour < 12 ? "AM" : "PM" );
return buf.toString();
case 10: 
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.day, 2, buf);
buf.append('-');
formatNumber( details.year % 100, 2, buf);
return buf.toString();
case 110: 
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.day, 2, buf);
buf.append('-');
formatNumber( details.year, 4, buf);
return buf.toString();
case 11: 
formatNumber( details.year % 100, 2, buf);
buf.append('/');
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.day, 2, buf);
return buf.toString();
case 111: 
formatNumber( details.year, 4, buf);
buf.append('/');
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.day, 2, buf);
return buf.toString();
case 12: 
formatNumber( details.year % 100, 2, buf);
formatNumber( details.month+1, 2, buf);
formatNumber( details.day, 2, buf);
return buf.toString();
case 112: 
formatNumber( details.year, 4, buf);
formatNumber( details.month+1, 2, buf);
formatNumber( details.day, 2, buf);
return buf.toString();
case 13:
case 113: 
formatNumber( details.day, 2, buf);
buf.append(' ');
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.year, 4, buf);
buf.append(' ');
formatNumber( details.hour, 2, buf );
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append(':');
formatMillis( details.millis, buf);
return buf.toString();
case 14:
case 114: 
formatNumber( details.hour, 2, buf);
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append(':');
formatMillis( details.millis, buf );
return buf.toString();
case 20:
case 120: 
formatNumber( details.year, 4, buf);
buf.append('-');
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.day, 2, buf);
buf.append(' ');
formatNumber( details.hour, 2, buf);
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
return buf.toString();
case 21:
case 121: 
formatNumber( details.year, 4, buf);
buf.append('-');
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.day, 2, buf);
buf.append(' ');
formatNumber( details.hour, 2, buf);
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append('.');
formatMillis( details.millis, buf );
return buf.toString();
case 26:
case 126: 
formatNumber( details.year, 4, buf);
buf.append('-');
formatNumber( details.month+1, 2, buf);
buf.append('-');
formatNumber( details.day, 2, buf);
buf.append('T');
formatNumber( details.hour, 2, buf);
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append('.');
formatMillis( details.millis, buf );
return buf.toString();
case 130: 
formatNumber( details.day, 2, buf);
buf.append(' ');
buf.append( SHORT_MONTHS[ details.month ]);
buf.append(' ');
formatNumber( details.year, 4, buf);
buf.append(' ');
formatHour12( details.hour, buf );
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append(':');
formatMillis( details.millis, buf);
buf.append( details.hour < 12 ? "AM" : "PM" );
return buf.toString();
case 131: 
formatNumber( details.day, 2, buf);
buf.append('/');
formatNumber( details.month+1, 2, buf);
buf.append('/');
formatNumber( details.year % 100, 2, buf);
buf.append(' ');
formatNumber( details.hour, 2, buf);
buf.append(':');
formatNumber( details.minute, 2, buf);
buf.append(':');
formatNumber( details.second, 2, buf);
buf.append(':');
formatMillis( details.millis, buf );
return buf.toString();
default:
return toString();
}
}
private final static void formatNumber(int value, int digitCount, StringBuffer buf){
buf.setLength(buf.length() + digitCount);
if(value < 0) value = - value;
for(int i=1; i<=digitCount; i++){
buf.setCharAt( buf.length()-i, Utils.digits[ value % 10 ] );
value /= 10;
}
}
private final static void formatMillis(int millis,  StringBuffer buf){
buf.append(Utils.digits[ (millis / 100) % 10 ]);
int value = millis % 100;
if(value != 0){
buf.append(Utils.digits[ value / 10 ]);
value %= 10;
if(value != 0)
buf.append(Utils.digits[ value ]);
}
}
private final static void formatHour12(int hour,  StringBuffer buf){
hour %= 12;
if(hour == 0) hour = 12;
formatNumber( hour, 2, buf );
}
private final static long addDateTimeOffset(long datetime){
return addDateTimeOffset( datetime, TimeZone.getDefault());
}
final static long addDateTimeOffset(long datetime, TimeZone timezone){
int t = (int)(datetime % 86400000);
int d = (int)(datetime / 86400000);
if(t<0){
t += 86400000;
d--;
}
int millis = t % 1000;
t /= 1000;
synchronized(cal){
cal.setTimeZone( timezone );
cal.set( 1970, 0, d+1, 0, 0, t );
cal.set( Calendar.MILLISECOND, millis );
return cal.getTimeInMillis();
}
}
private static long removeDateTimeOffset(long datetime){
synchronized(cal){
cal.setTimeZone( TimeZone.getDefault() );
cal.setTimeInMillis( datetime );
return datetime + cal.get( Calendar.ZONE_OFFSET) + cal.get( Calendar.DST_OFFSET);
}
}
static Timestamp getTimestamp(long time){
return new Timestamp( DateTime.addDateTimeOffset(time) );
}
static Time getTime(long time){
return new Time( DateTime.addDateTimeOffset(time) );
}
static Date getDate(long time){
return new Date( DateTime.addDateTimeOffset(time) );
}
public Object getImmutableObject(){
switch(dataType){
case SQLTokenizer.DATE:
return getDate( time );
case SQLTokenizer.TIME:
return getTime( time );
default:
return getTimestamp( time );
}
}
static class Details{
int year;
int month;
int dayofyear;
int day;
int hour;
int minute;
int second;
int millis;
Details(long time){
int t = (int)(time % 86400000);
int d = (int)(time / 86400000);
if(t<0){
t += 86400000;
d--;
}
millis = t % 1000;
t /= 1000;
second = t % 60;
t /= 60;
minute = t % 60;
t /= 60;
hour = t % 24;
year = 1970 - (int)(t / 365.2425);
boolean isLeap;
do{
isLeap = false;
dayofyear = day = d - ((year - 1970)*365 + (year/4) - (year/100) + (year/400) - 477);
if(isLeapYear(year)){
if(day < 59){
day++;
isLeap = true;
}
dayofyear++;
}
if(day < 0){
year--;
continue;
}else
if(day >= 365){
year++;
continue;
}
break;
}while(true);
if(isLeap && day == 59){
month = 1;
day   = 29;
}else{
for(int m=11; m>=0; m--){
if(MONTH_DAYS[m] <= day){
month = m;
day   = day - MONTH_DAYS[m] + 1;
break;
}
}
}
}
}
static boolean isLeapYear(int year){
return year % 4 == 0 && (year%100 != 0 || year%400 == 0);
}
private static final Calendar cal = Calendar.getInstance();
}
package smallsql.database;
public class ExpressionFunctionTimestampAdd extends ExpressionFunction {
final private int interval;
ExpressionFunctionTimestampAdd(int intervalType, Expression p1, Expression p2){
interval = ExpressionFunctionTimestampDiff.mapIntervalType( intervalType );
setParams( new Expression[]{p1,p2});
}
int getFunction() {
return SQLTokenizer.TIMESTAMPADD;
}
boolean isNull() throws Exception {
return param1.isNull() || param2.isNull();
}
boolean getBoolean() throws Exception {
return getLong() != 0;
}
int getInt() throws Exception {
return (int)getLong();
}
long getLong() throws Exception {
if(isNull()) return 0;
switch(interval){
case SQLTokenizer.SQL_TSI_FRAC_SECOND:
return param2.getLong() + param1.getLong();
case SQLTokenizer.SQL_TSI_SECOND:
return param2.getLong() + param1.getLong() * 1000;
case SQLTokenizer.SQL_TSI_MINUTE:
return param2.getLong() + param1.getLong() * 60000;
case SQLTokenizer.SQL_TSI_HOUR:
return param2.getLong() + param1.getLong() * 3600000;
case SQLTokenizer.SQL_TSI_DAY:
return param2.getLong() + param1.getLong() * 86400000;
case SQLTokenizer.SQL_TSI_WEEK:{
return param2.getLong() + param1.getLong() * 604800000;
}case SQLTokenizer.SQL_TSI_MONTH:{
DateTime.Details details2 = new DateTime.Details(param2.getLong());
details2.month += param1.getLong();
return DateTime.calcMillis(details2);
}
case SQLTokenizer.SQL_TSI_QUARTER:{
DateTime.Details details2 = new DateTime.Details(param2.getLong());
details2.month += param1.getLong() * 3;
return DateTime.calcMillis(details2);
}
case SQLTokenizer.SQL_TSI_YEAR:{
DateTime.Details details2 = new DateTime.Details(param2.getLong());
details2.year += param1.getLong();
return DateTime.calcMillis(details2);
}
default: throw new Error();
}
}
float getFloat() throws Exception {
return getLong();
}
double getDouble() throws Exception {
return getLong();
}
long getMoney() throws Exception {
return getLong() * 10000;
}
MutableNumeric getNumeric() throws Exception {
if(isNull()) return null;
return new MutableNumeric(getLong());
}
Object getObject() throws Exception {
if(isNull()) return null;
return new DateTime( getLong(), SQLTokenizer.TIMESTAMP );
}
String getString() throws Exception {
if(isNull()) return null;
return new DateTime( getLong(), SQLTokenizer.TIMESTAMP ).toString();
}
int getDataType() {
return SQLTokenizer.TIMESTAMP;
}
}
package smallsql.junit;
import junit.framework.*;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DateFormatSymbols;
public class BasicTestCase extends TestCase {
}
public void assertRSMetaData( ResultSet rs, String[] colNames, int[] types) throws Exception{
ResultSetMetaData rm = rs.getMetaData();
int count = rm.getColumnCount();
assertEquals( "Column count:", colNames.length, count);
for(int i=1; i<=count; i++){
assertEquals("Col "+i+" name", colNames[i-1], rm.getColumnName(i));
assertEquals("Col "+i+" label", colNames[i-1], rm.getColumnLabel(i));
assertEquals("Col "+i+" type", types   [i-1], rm.getColumnType(i));
switch(types[i-1]){
case Types.VARCHAR:
assertTrue  ("Wrong Precision (" + rm.getColumnTypeName(i) + ") for Column "+i+": "+rm.getPrecision(i), rm.getPrecision(i) > 0);
break;
case Types.INTEGER:
assertTrue  ("Wrong Precision (" + rm.getColumnTypeName(i) + ") for Column "+i, rm.getPrecision(i) > 0);
break;
}
}
}
private final static char[] digits = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
private static String bytes2hex( byte[] bytes ){
StringBuffer buf = new StringBuffer(bytes.length << 1);
for(int i=0; i<bytes.length; i++){
buf.append( digits[ (bytes[i] >> 4) & 0x0F ] );
buf.append( digits[ (bytes[i]     ) & 0x0F ] );
}
return buf.toString();
}
public void assertEqualsObject( String msg, Object obj1, Object obj2 ){
if(obj1 instanceof byte[]){
if(!java.util.Arrays.equals( (byte[])obj1, (byte[])obj2)){
fail(msg + " expected:" + bytes2hex((byte[])obj1)+ " but was:"+bytes2hex((byte[])obj2));
}
}else{
if(obj1 instanceof BigDecimal)
if(((BigDecimal)obj1).compareTo((BigDecimal)obj2) == 0) return;
assertEquals( msg, obj1, obj2);
}
}
public void assertEqualsObject( String msg, Object obj1, Object obj2, boolean needTrim ){
if(needTrim && obj1 != null){
if(obj1 instanceof String) obj1 = ((String)obj1).trim();
if(obj1 instanceof byte[]){
byte[] tmp = (byte[])obj1;
int k=tmp.length-1;
for(; k>= 0; k--) if(tmp[k] != 0) break;
k++;
byte[] tmp2 = new byte[k];
System.arraycopy( tmp, 0, tmp2, 0, k);
obj1 = tmp2;
}
}
if(needTrim && obj2 != null){
if(obj2 instanceof String) obj2 = ((String)obj2).trim();
if(obj2 instanceof byte[]){
byte[] tmp = (byte[])obj2;
int k=tmp.length-1;
for(; k>= 0; k--) if(tmp[k] != 0) break;
k++;
byte[] tmp2 = new byte[k];
System.arraycopy( tmp, 0, tmp2, 0, k);
obj2 = tmp2;
}
}
assertEqualsObject( msg, obj1, obj2);
}
void assertRowCount(int sollCount, String sql ) throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery(sql);
assertRowCount(sollCount,rs);
}
void assertRowCount(int sollCount, ResultSet rs ) throws Exception{
int colCount = rs.getMetaData().getColumnCount();
int count = 0;
while(rs.next()){
count++;
for(int i=1; i<=colCount; i++){
rs.getObject(i);
}
}
assertEquals( "Wrong row count", sollCount, count);
for(int i=1; i<=colCount; i++){
try{
fail( "Column:"+i+" Value:"+String.valueOf(rs.getObject(i)));
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
}
assertFalse( "Scroll after last", rs.next() );
}
private boolean string2boolean( String val){
try{
return Double.parseDouble( val ) != 0;
}catch(NumberFormatException e){
return "true".equalsIgnoreCase( val ) || "yes".equalsIgnoreCase( val ) || "t".equalsIgnoreCase( val );
}
void assertEqualsRsValue(Object obj, String sql) throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery(sql);
assertTrue( "No row produce", rs.next());
assertEqualsRsValue(obj,rs,false);
}
void assertEqualsRsValue(Object obj, ResultSet rs, boolean needTrim) throws Exception{
String name = rs.getMetaData().getColumnName(1);
assertEqualsObject( "Values not identical on read:", obj, rs.getObject(name), needTrim);
if(obj instanceof Time){
assertEquals("Time is different:", obj, rs.getTime(name) );
assertEquals("Time String is different:", obj.toString(), rs.getString(name) );
}
if(obj instanceof Timestamp){
assertEquals("Timestamp is different:", obj, rs.getTimestamp(name) );
assertEquals("Timestamp String is different:", obj.toString(), rs.getString(name) );
}
if(obj instanceof Date){
assertEquals("Date is different:", obj, rs.getDate(name) );
assertEquals("Date String is different:", obj.toString(), rs.getString(name) );
}
if(obj instanceof String){
String str = (String)obj;
assertEqualsObject("String is different:", str, rs.getString(name), needTrim );
assertEquals("String Boolean is different:", string2boolean(str), rs.getBoolean(name) );
try{
assertEquals("String Long is different:", Long.parseLong(str), rs.getLong(name) );
}catch(NumberFormatException ex){
try{
assertEquals("String Integer is different:", Integer.parseInt(str), rs.getInt(name) );
}catch(NumberFormatException ex){
try{
assertEquals("String Float is different:", Float.parseFloat(str), rs.getFloat(name), 0.0 );
}catch(NumberFormatException ex){
try{
assertEquals("String Double is different:", Double.parseDouble(str), rs.getDouble(name), 0.0 );
}catch(NumberFormatException ex){
}
if(obj instanceof BigDecimal){
if(!needTrim){
assertEquals("BigDecimal is different:", obj, rs.getBigDecimal(name) );
assertEquals("Scale is different:", ((BigDecimal)obj).scale(), rs.getMetaData().getScale(1));
}
assertEquals("Scale Meta is different:", rs.getBigDecimal(name).scale(), rs.getMetaData().getScale(1));
BigDecimal big2 = ((BigDecimal)obj).setScale(2,BigDecimal.ROUND_HALF_EVEN);
assertEquals("BigDecimal mit scale is different:", big2, rs.getBigDecimal(name, 2) );
}
if(obj instanceof Integer){
assertEquals("Scale is different:", 0, rs.getMetaData().getScale(1));
}
if(obj instanceof Number){
long longValue = ((Number)obj).longValue();
int intValue = ((Number)obj).intValue();
if(longValue >= Integer.MAX_VALUE)
intValue = Integer.MAX_VALUE;
if(longValue <= Integer.MIN_VALUE)
intValue = Integer.MIN_VALUE;
assertEquals("int is different:", intValue, rs.getInt(name) );
assertEquals("long is different:", longValue, rs.getLong(name) );
if(intValue >= Short.MIN_VALUE && intValue <= Short.MAX_VALUE)
assertEquals("short is different:", (short)intValue, rs.getShort(name) );
if(intValue >= Byte.MIN_VALUE && intValue <= Byte.MAX_VALUE)
assertEquals("byte is different:", (byte)intValue, rs.getByte(name) );
double value = ((Number)obj).doubleValue();
assertEquals("Double is different:", value, rs.getDouble(name),0.0 );
assertEquals("Float is different:", (float)value, rs.getFloat(name),0.0 );
String valueStr = obj.toString();
if(!needTrim){
assertEquals("Number String is different:", valueStr, rs.getString(name) );
}
BigDecimal decimal = Double.isInfinite(value) || Double.isNaN(value) ? null : new BigDecimal(valueStr);
assertEqualsObject("Number BigDecimal is different:", decimal, rs.getBigDecimal(name) );
assertEquals("Number boolean is different:", value != 0, rs.getBoolean(name) );
}
if(obj == null){
assertNull("String is different:", rs.getString(name) );
assertNull("Date is different:", rs.getDate(name) );
assertNull("Time is different:", rs.getTime(name) );
assertNull("Timestamp is different:", rs.getTimestamp(name) );
assertNull("BigDecimal is different:", rs.getBigDecimal(name) );
assertNull("BigDecimal with scale is different:", rs.getBigDecimal(name, 2) );
assertNull("Bytes with scale is different:", rs.getBytes(name) );
assertEquals("Double is different:", 0, rs.getDouble(name),0 );
assertEquals("Float is different:", 0, rs.getFloat(name),0 );
assertEquals("Long is different:", 0, rs.getLong(name) );
assertEquals("Int is different:", 0, rs.getInt(name) );
assertEquals("SmallInt is different:", 0, rs.getShort(name) );
assertEquals("TinyInt is different:", 0, rs.getByte(name) );
assertEquals("Boolean is different:", false, rs.getBoolean(name) );
}
if(obj instanceof byte[]){
assertTrue("Binary should start with 0x", rs.getString(name).startsWith("0x"));
}
ResultSetMetaData metaData = rs.getMetaData();
String className = metaData.getColumnClassName(1);
assertNotNull( "ClassName:", className);
if(obj != null){
Class gotClass = Class.forName(className);
Class objClass = obj.getClass();
String objClassName = objClass.getName();
int expectedLen = metaData.getColumnDisplaySize(1);
if (gotClass.equals(java.sql.Blob.class)) {
assertTrue(
"ClassName assignable: "+className+"<->"+objClassName,
objClass.equals(new byte[0].getClass()));
String message = "Check DisplaySize: " + expectedLen + "!=" + Integer.MAX_VALUE + ")";
assertTrue( message, expectedLen == Integer.MAX_VALUE );
}
else if (gotClass.equals(java.sql.Clob.class)) { 
assertTrue(
"ClassName assignable: "+className+"<->"+objClassName,
objClass.equals(String.class));
String message = "Check DisplaySize: " + expectedLen + "!=" + Integer.MAX_VALUE + ")";
assertTrue( message, expectedLen == Integer.MAX_VALUE );
}
else {
String foundStr = rs.getString(name);
assertTrue("ClassName assignable: "+className+"<->"+objClassName, gotClass.isAssignableFrom(objClass));
assertTrue( "DisplaySize to small "+ expectedLen +"<"+foundStr.length()+" (" + foundStr + ")", expectedLen >= foundStr.length() );
}
}
}
void assertSQLException(String sqlstate, int vendorCode, SQLException ex) {
StringWriter sw = new StringWriter();
ex.printStackTrace(new PrintWriter(sw));
assertEquals( "Vendor Errorcode:"+sw, vendorCode, ex.getErrorCode() );
assertEquals( "SQL State:"+sw, sqlstate, ex.getSQLState());
}
void printSQL(String sql) throws SQLException{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery(sql);
printRS( rs );
}
void printRS(ResultSet rs) throws SQLException{
int count = rs.getMetaData().getColumnCount();
while(rs.next()){
for(int i=1; i<=count; i++){
System.out.print(rs.getString(i) + '\t');
}
System.out.println();
}
}
static String getMonth3L(int ordinal) {
return MONTHS[ordinal - 1];
}
}
package smallsql.database;
abstract class ExpressionFunctionReturnInt extends ExpressionFunction {
boolean isNull() throws Exception {
return param1.isNull();
}
final boolean getBoolean() throws Exception {
return getInt() != 0;
}
final long getLong() throws Exception {
return getInt();
}
final float getFloat() throws Exception {
return getInt();
}
final double getDouble() throws Exception {
return getInt();
}
final long getMoney() throws Exception {
return getInt() * 10000;
}
final MutableNumeric getNumeric() throws Exception {
if(isNull()) return null;
return new MutableNumeric(getInt());
}
Object getObject() throws Exception {
if(isNull()) return null;
return Utils.getInteger(getInt());
}
final String getString() throws Exception {
if(isNull()) return null;
return String.valueOf(getInt());
}
final int getDataType() {
return SQLTokenizer.INT;
}
}
package smallsql.database.language;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
public class Language {
public static final String CUSTOM_MESSAGE			= "SS-0000";
public static final String UNSUPPORTED_OPERATION 	= "SS-0001";
public static final String CANT_LOCK_FILE           = "SS-0003";
public static final String DB_EXISTENT 				= "SS-0030";
public static final String DB_NONEXISTENT 			= "SS-0031";
public static final String DB_NOT_DIRECTORY 		= "SS-0032";
public static final String DB_NOTCONNECTED 			= "SS-0033";
public static final String DB_READONLY              = "SS-0034";
public static final String CONNECTION_CLOSED 		= "SS-0070";
public static final String VIEW_INSERT 				= "SS-0100";
public static final String VIEWDROP_NOT_VIEW 		= "SS-0101";
public static final String VIEW_CANTDROP 			= "SS-0102";
public static final String RSET_NOT_PRODUCED 		= "SS-0130";
public static final String RSET_READONLY 			= "SS-0131";
public static final String RSET_FWDONLY				= "SS-0132";
public static final String RSET_CLOSED				= "SS-0133";
public static final String RSET_NOT_INSERT_ROW		= "SS-0134";
public static final String RSET_ON_INSERT_ROW		= "SS-0135";
public static final String ROWSOURCE_READONLY		= "SS-0136";
public static final String STMT_IS_CLOSED           = "SS-0140";
public static final String SUBQUERY_COL_COUNT		= "SS-0160";
public static final String JOIN_DELETE				= "SS-0161";
public static final String JOIN_INSERT				= "SS-0162";
public static final String DELETE_WO_FROM			= "SS-0163";
public static final String INSERT_WO_FROM			= "SS-0164";
public static final String TABLE_CANT_RENAME		= "SS-0190";
public static final String TABLE_CANT_DROP			= "SS-0191";
public static final String TABLE_CANT_DROP_LOCKED	= "SS-0192";
public static final String TABLE_CORRUPT_PAGE		= "SS-0193";
public static final String TABLE_MODIFIED			= "SS-0194";
public static final String TABLE_DEADLOCK			= "SS-0195";
public static final String TABLE_OR_VIEW_MISSING	= "SS-0196";
public static final String TABLE_FILE_INVALID		= "SS-0197";
public static final String TABLE_OR_VIEW_FILE_INVALID = "SS-0198";
public static final String TABLE_EXISTENT			= "SS-0199";
public static final String FK_NOT_TABLE				= "SS-0220";
public static final String PK_ONLYONE				= "SS-0221";
public static final String KEY_DUPLICATE			= "SS-0222";
public static final String MONTH_TOOLARGE 			= "SS-0251";
public static final String DAYS_TOOLARGE 			= "SS-0252";
public static final String HOURS_TOOLARGE 			= "SS-0253";
public static final String MINUTES_TOOLARGE 		= "SS-0254";
public static final String SECS_TOOLARGE 			= "SS-0255";
public static final String MILLIS_TOOLARGE 			= "SS-0256";
public static final String DATETIME_INVALID 		= "SS-0257";
public static final String UNSUPPORTED_CONVERSION_OPER = "SS-0280";
public static final String UNSUPPORTED_DATATYPE_OPER = "SS-0281";
public static final String UNSUPPORTED_DATATYPE_FUNC = "SS-0282";
public static final String UNSUPPORTED_CONVERSION_FUNC = "SS-0283";
public static final String UNSUPPORTED_TYPE_CONV 	= "SS-0284";
public static final String UNSUPPORTED_TYPE_SUM 	= "SS-0285";
public static final String UNSUPPORTED_TYPE_MAX 	= "SS-0286";
public static final String UNSUPPORTED_CONVERSION 	= "SS-0287";
public static final String INSERT_INVALID_LEN 		= "SS-0288";
public static final String SUBSTR_INVALID_LEN 		= "SS-0289";
public static final String VALUE_STR_TOOLARGE 		= "SS-0310";
public static final String VALUE_BIN_TOOLARGE 		= "SS-0311";
public static final String VALUE_NULL_INVALID 		= "SS-0312";
public static final String VALUE_CANT_CONVERT 		= "SS-0313";
public static final String BYTEARR_INVALID_SIZE 	= "SS-0340";
public static final String LOB_DELETED 				= "SS-0341";
public static final String PARAM_CLASS_UNKNOWN 		= "SS-0370";
public static final String PARAM_EMPTY 				= "SS-0371";
public static final String PARAM_IDX_OUT_RANGE 		= "SS-0372";
public static final String COL_DUPLICATE 			= "SS-0400";
public static final String COL_MISSING 				= "SS-0401";
public static final String COL_VAL_UNMATCH 			= "SS-0402";
public static final String COL_INVALID_SIZE 		= "SS-0403";
public static final String COL_WRONG_PREFIX 		= "SS-0404";
public static final String COL_READONLY 			= "SS-0405";
public static final String COL_INVALID_NAME 		= "SS-0406";
public static final String COL_IDX_OUT_RANGE 		= "SS-0407";
public static final String COL_AMBIGUOUS 			= "SS-0408";
public static final String GROUP_AGGR_INVALID 		= "SS-0430";
public static final String GROUP_AGGR_NOTPART 		= "SS-0431";
public static final String ORDERBY_INTERNAL 		= "SS-0432";
public static final String UNION_DIFFERENT_COLS 	= "SS-0433";
public static final String INDEX_EXISTS 			= "SS-0460";
public static final String INDEX_MISSING 			= "SS-0461";
public static final String INDEX_FILE_INVALID 		= "SS-0462";
public static final String INDEX_CORRUPT 			= "SS-0463";
public static final String INDEX_TOOMANY_EQUALS 	= "SS-0464";
public static final String FILE_TOONEW 				= "SS-0490";
public static final String FILE_TOOOLD 				= "SS-0491";
public static final String FILE_CANT_DELETE         = "SS-0492";
public static final String ROW_0_ABSOLUTE 			= "SS-0520";
public static final String ROW_NOCURRENT 			= "SS-0521";
public static final String ROWS_WRONG_MAX 			= "SS-0522";
public static final String ROW_LOCKED 				= "SS-0523";
public static final String ROW_DELETED 				= "SS-0524";
public static final String SAVEPT_INVALID_TRANS 	= "SS-0550";
public static final String SAVEPT_INVALID_DRIVER 	= "SS-0551";
public static final String ALIAS_UNSUPPORTED 		= "SS-0580";
public static final String ISOLATION_UNKNOWN 		= "SS-0581";
public static final String FLAGVALUE_INVALID 		= "SS-0582";
public static final String ARGUMENT_INVALID 		= "SS-0583";
public static final String GENER_KEYS_UNREQUIRED 	= "SS-0584";
public static final String SEQUENCE_HEX_INVALID 	= "SS-0585";
public static final String SEQUENCE_HEX_INVALID_STR = "SS-0586";
public static final String SYNTAX_BASE_OFS			= "SS-0610";
public static final String SYNTAX_BASE_END			= "SS-0611";
public static final String STXADD_ADDITIONAL_TOK	= "SS-0612";
public static final String STXADD_IDENT_EXPECT		= "SS-0613";
public static final String STXADD_IDENT_EMPTY		= "SS-0614";
public static final String STXADD_IDENT_WRONG		= "SS-0615";
public static final String STXADD_OPER_MINUS		= "SS-0616";
public static final String STXADD_FUNC_UNKNOWN		= "SS-0617";
public static final String STXADD_PARAM_INVALID_COUNT	= "SS-0618";
public static final String STXADD_JOIN_INVALID		= "SS-0619";
public static final String STXADD_FROM_PAR_CLOSE	= "SS-0620";
public static final String STXADD_KEYS_REQUIRED		= "SS-0621";
public static final String STXADD_NOT_NUMBER		= "SS-0622";
public static final String STXADD_COMMENT_OPEN		= "SS-0623";
private Map messages;
private Map sqlStates;
public static Language getLanguage(String localeStr) {
try {
return getFromLocaleTree(localeStr);
}
catch (IllegalArgumentException e) {
return getDefaultLanguage();
}
}
public static Language getDefaultLanguage() {
String dfltLocaleStr = Locale.getDefault().toString();
try {
return getFromLocaleTree(dfltLocaleStr);
}
catch (IllegalArgumentException e) {
return new Language(); 
}
}
private static Language getFromLocaleTree(String localeStr)
throws IllegalArgumentException {
String part = localeStr;
while (true) {
String langClassName = Language.class.getName() + '_' + part;
try {
return (Language) Class.forName(langClassName).newInstance();
}
catch (IllegalAccessException e) {
assert(false): "Internal error: must never happen.";
}
catch (ClassNotFoundException e) {
}
catch (InstantiationException e) {
assert(false): "Error during Language instantiation: " + e.getMessage();
}
int lastUndsc = part.lastIndexOf("_");
if (lastUndsc > -1) part = part.substring(0, lastUndsc);
else break;
}
throw new IllegalArgumentException("Locale not found in the tree: " + localeStr);
}
protected Language() {
messages = new HashMap((int)(MESSAGES.length / 0.7)); 
sqlStates = new HashMap((int)(MESSAGES.length / 0.7)); 
addMessages(MESSAGES);
setSqlStates();
}
protected final void addMessages(String[][] entries)
throws IllegalArgumentException {
Set inserted = new HashSet(); 
for (int i = 0; i < entries.length; i++) {
String key = entries[i][0];
if (! inserted.add(key)) {
throw new IllegalArgumentException("Duplicate key: " + key);
}
else {
String value = entries[i][1];
messages.put(key, value);
}
}
}
private final void setSqlStates() {
Set inserted = new HashSet(); 
for (int i = 0; i < SQL_STATES.length; i++) {
String key = SQL_STATES[i][0];
if (! inserted.add(key)) {
throw new IllegalArgumentException("Duplicate key: " + key);
}
else {
String value = SQL_STATES[i][1];
sqlStates.put(key, value);
}
}
}
public String getMessage(String key) {
String message = (String) messages.get(key);
assert(message != null): "Message code not found: " + key;
return message;
}
public String getSqlState(String key) {
String sqlState = (String) sqlStates.get(key);
assert(sqlState != null): "SQL State code not found: " + key;
return sqlState;
}
public String[][] getEntries() {
return MESSAGES;
}
private final String[][] MESSAGES = {
{ CUSTOM_MESSAGE           		  , "{0}" },
{ UNSUPPORTED_OPERATION           , "Unsupported Operation {0}." },
{ CANT_LOCK_FILE                  , "Can''t lock file ''{0}''. A single SmallSQL Database can only be opened from a single process." },
{ DB_EXISTENT                     , "Database ''{0}'' already exists." },
{ DB_NONEXISTENT                  , "Database ''{0}'' does not exist." },
{ DB_NOT_DIRECTORY                , "Directory ''{0}'' is not a SmallSQL database." },
{ DB_NOTCONNECTED                 , "You are not connected with a Database." },
{ CONNECTION_CLOSED               , "Connection is already closed." },
{ VIEW_INSERT                     , "INSERT is not supported for a view." },
{ VIEWDROP_NOT_VIEW               , "Cannot use DROP VIEW with ''{0}'' because it does not is a view." },
{ VIEW_CANTDROP                   , "View ''{0}'' can''t be dropped." },
{ RSET_NOT_PRODUCED               , "No ResultSet was produced." },
{ RSET_READONLY                   , "ResultSet is read only." },
{ RSET_FWDONLY                    , "ResultSet is forward only." },
{ RSET_CLOSED                     , "ResultSet is closed." },
{ RSET_NOT_INSERT_ROW             , "Cursor is currently not on the insert row." },
{ RSET_ON_INSERT_ROW              , "Cursor is currently on the insert row." },
{ ROWSOURCE_READONLY              , "Rowsource is read only." },
{ STMT_IS_CLOSED                  , "Statement is already closed." },
{ SUBQUERY_COL_COUNT              , "Count of columns in subquery must be 1 and not {0}." },
{ JOIN_DELETE                     , "The method deleteRow not supported on joins." },
{ JOIN_INSERT                     , "The method insertRow not supported on joins." },
{ DELETE_WO_FROM                  , "The method deleteRow need a FROM expression." },
{ INSERT_WO_FROM                  , "The method insertRow need a FROM expression." },
{ TABLE_CANT_RENAME               , "Table ''{0}'' can''t be renamed." },
{ TABLE_CANT_DROP                 , "Table ''{0}'' can''t be dropped." },
{ TABLE_CANT_DROP_LOCKED          , "Table ''{0}'' can''t drop because is locked." },
{ TABLE_CORRUPT_PAGE              , "Corrupt table page at position: {0}." },
{ TABLE_MODIFIED                  , "Table ''{0}'' was modified." },
{ TABLE_DEADLOCK                  , "Deadlock, can not create a lock on table ''{0}''." },
{ TABLE_OR_VIEW_MISSING           , "Table or View ''{0}'' does not exist." },
{ TABLE_FILE_INVALID              , "File ''{0}'' does not include a valid SmallSQL Table." },
{ TABLE_OR_VIEW_FILE_INVALID      , "File ''{0}'' is not a valid Table or View store." },
{ TABLE_EXISTENT                  , "Table or View ''{0}'' already exists." },
{ FK_NOT_TABLE                    , "''{0}'' is not a table." },
{ PK_ONLYONE                      , "A table can have only one primary key." },
{ KEY_DUPLICATE                   , "Duplicate Key." },
{ MONTH_TOOLARGE                  , "Months are too large in DATE or TIMESTAMP value ''{0}''." },
{ DAYS_TOOLARGE                   , "Days are too large in DATE or TIMESTAMP value ''{0}''." },
{ HOURS_TOOLARGE                  , "Hours are too large in TIME or TIMESTAMP value ''{0}''." },
{ MINUTES_TOOLARGE                , "Minutes are too large in TIME or TIMESTAMP value ''{0}''." },
{ SECS_TOOLARGE                   , "Seconds are too large in TIME or TIMESTAMP value ''{0}''." },
{ MILLIS_TOOLARGE                 , "Milliseconds are too large in TIMESTAMP value ''{0}''." },
{ DATETIME_INVALID                , "''{0}'' is an invalid DATE, TIME or TIMESTAMP." },
{ UNSUPPORTED_CONVERSION_OPER     , "Unsupported conversion to data type ''{0}'' from data type ''{1}'' for operation ''{2}''." },
{ UNSUPPORTED_DATATYPE_OPER       , "Unsupported data type ''{0}'' for operation ''{1}''." },
{ UNSUPPORTED_DATATYPE_FUNC       , "Unsupported data type ''{0}'' for function ''{1}''." },
{ UNSUPPORTED_CONVERSION_FUNC     , "Unsupported conversion to data type ''{0}'' for function ''{1}''." },
{ UNSUPPORTED_TYPE_CONV           , "Unsupported type for CONVERT function: {0}." },
{ UNSUPPORTED_TYPE_SUM            , "Unsupported data type ''{0}'' for SUM function." },
{ UNSUPPORTED_TYPE_MAX            , "Unsupported data type ''{0}'' for MAX function." },
{ UNSUPPORTED_CONVERSION          , "Can''t convert ''{0}'' [{1}] to ''{2}''." },
{ INSERT_INVALID_LEN              , "Invalid length ''{0}'' in function INSERT." },
{ SUBSTR_INVALID_LEN              , "Invalid length ''{0}'' in function SUBSTRING." },
{ VALUE_STR_TOOLARGE              , "String value too large for column." },
{ VALUE_BIN_TOOLARGE              , "Binary value with length {0} to large for column with size {1}." },
{ VALUE_NULL_INVALID              , "Null values are not valid for column ''{0}''." },
{ VALUE_CANT_CONVERT              , "Cannot convert a {0} value to a {1} value." },
{ BYTEARR_INVALID_SIZE            , "Invalid byte array size {0} for UNIQUEIDENFIER." },
{ LOB_DELETED                     , "Lob Object was deleted." },
{ PARAM_CLASS_UNKNOWN             , "Unknown parameter class: ''{0}''." },
{ PARAM_EMPTY                     , "Parameter {0} is empty." },
{ PARAM_IDX_OUT_RANGE             , "Parameter index {0} out of range. The value must be between 1 and {1}." },
{ COL_DUPLICATE                	  , "There is a duplicated column name: ''{0}''." },
{ COL_MISSING                     , "Column ''{0}'' not found." },
{ COL_VAL_UNMATCH                 , "Columns and Values count is not identical." },
{ COL_INVALID_SIZE                , "Invalid column size {0} for column ''{1}''." },
{ COL_WRONG_PREFIX                , "The column prefix ''{0}'' does not match with a table name or alias name used in this query." },
{ COL_READONLY                    , "Column {0} is read only." },
{ COL_INVALID_NAME                , "Invalid column name ''{0}''." },
{ COL_IDX_OUT_RANGE               , "Column index out of range: {0}." },
{ COL_AMBIGUOUS                   , "Column ''{0}'' is ambiguous." },
{ GROUP_AGGR_INVALID              , "Aggregate function are not valid in the GROUP BY clause ({0})." },
{ GROUP_AGGR_NOTPART              , "Expression ''{0}'' is not part of a aggregate function or GROUP BY clause." },
{ ORDERBY_INTERNAL                , "Internal Error with ORDER BY." },
{ UNION_DIFFERENT_COLS            , "Different SELECT of the UNION have different column count: {0} and {1}." },
{ INDEX_EXISTS                    , "Index ''{0}'' already exists." },
{ INDEX_MISSING                   , "Index ''{0}'' does not exist." },
{ INDEX_FILE_INVALID              , "File ''{0}'' is not a valid Index store." },
{ INDEX_CORRUPT                   , "Error in loading Index. Index file is corrupt. ({0})." },
{ INDEX_TOOMANY_EQUALS            , "Too many equals entry in Index." },
{ FILE_TOONEW                     , "File version ({0}) of file ''{1}'' is too new for this runtime." },
{ FILE_TOOOLD                     , "File version ({0}) of file ''{1}'' is too old for this runtime." },
{ FILE_CANT_DELETE                , "File ''{0}'' can't be deleted." },
{ ROW_0_ABSOLUTE                  , "Row 0 is invalid for method absolute()." },
{ ROW_NOCURRENT                   , "No current row." },
{ ROWS_WRONG_MAX                  , "Wrong max rows value: {0}." },
{ ROW_LOCKED                      , "Row is locked from another Connection." },
{ ROW_DELETED                     , "Row already deleted." },
{ SAVEPT_INVALID_TRANS            , "Savepoint is not valid for this transaction." },
{ SAVEPT_INVALID_DRIVER           , "Savepoint is not valid for this driver {0}." },
{ ALIAS_UNSUPPORTED               , "Alias not supported for this type of row source." },
{ ISOLATION_UNKNOWN               , "Unknown Transaction Isolation Level: {0}." },
{ FLAGVALUE_INVALID               , "Invalid flag value in method getMoreResults: {0}." },
{ ARGUMENT_INVALID                , "Invalid argument in method setNeedGenratedKeys: {0}." },
{ GENER_KEYS_UNREQUIRED           , "GeneratedKeys not requested." },
{ SEQUENCE_HEX_INVALID            , "Invalid hex sequence at {0}." },
{ SEQUENCE_HEX_INVALID_STR        , "Invalid hex sequence at position {0} in ''{1}''." },
{ SYNTAX_BASE_OFS            	  , "Syntax error at offset {0} on ''{1}''. " },
{ SYNTAX_BASE_END        		  , "Syntax error, unexpected end of SQL string. " },
{ STXADD_ADDITIONAL_TOK			  , "Additional token after end of SQL statement." },
{ STXADD_IDENT_EXPECT			  , "Identifier expected." },
{ STXADD_IDENT_EMPTY 			  , "Empty Identifier." },
{ STXADD_IDENT_WRONG 			  , "Wrong Identifier ''{0}''." },
{ STXADD_OPER_MINUS 			  , "Invalid operator minus for data type VARBINARY." },
{ STXADD_FUNC_UNKNOWN 			  , "Unknown function." },
{ STXADD_PARAM_INVALID_COUNT	  , "Invalid parameter count." },
{ STXADD_JOIN_INVALID	  		  , "Invalid Join Syntax." },
{ STXADD_FROM_PAR_CLOSE	  		  , "Unexpected closing parenthesis in FROM clause." },
{ STXADD_KEYS_REQUIRED	  		  , "Required keywords are: " },
{ STXADD_NOT_NUMBER	  		      , "Number value required (passed = ''{0}'')." },
{ STXADD_COMMENT_OPEN			  , "Missing end comment mark (''*/'')." },
};
private final String[][] SQL_STATES = {
{ CUSTOM_MESSAGE           		  , "01000" },
{ UNSUPPORTED_OPERATION           , "01000" },
{ CANT_LOCK_FILE                  , "01000" },
{ DB_EXISTENT                     , "01000" },
{ DB_NONEXISTENT                  , "01000" },
{ DB_NOT_DIRECTORY                , "01000" },
{ DB_NOTCONNECTED                 , "01000" },
{ CONNECTION_CLOSED               , "01000" },
{ VIEW_INSERT                     , "01000" },
{ VIEWDROP_NOT_VIEW               , "01000" },
{ VIEW_CANTDROP                   , "01000" },
{ RSET_NOT_PRODUCED               , "01000" },
{ RSET_READONLY                   , "01000" },
{ RSET_FWDONLY                    , "01000" },
{ RSET_CLOSED                     , "01000" },
{ RSET_NOT_INSERT_ROW             , "01000" },
{ RSET_ON_INSERT_ROW              , "01000" },
{ ROWSOURCE_READONLY              , "01000" },
{ STMT_IS_CLOSED                  , "HY010" },
{ SUBQUERY_COL_COUNT              , "01000" },
{ JOIN_DELETE                     , "01000" },
{ JOIN_INSERT                     , "01000" },
{ DELETE_WO_FROM                  , "01000" },
{ INSERT_WO_FROM                  , "01000" },
{ TABLE_CANT_RENAME               , "01000" },
{ TABLE_CANT_DROP                 , "01000" },
{ TABLE_CANT_DROP_LOCKED          , "01000" },
{ TABLE_CORRUPT_PAGE              , "01000" },
{ TABLE_MODIFIED                  , "01000" },
{ TABLE_DEADLOCK                  , "01000" },
{ TABLE_OR_VIEW_MISSING           , "01000" },
{ TABLE_FILE_INVALID              , "01000" },
{ TABLE_OR_VIEW_FILE_INVALID      , "01000" },
{ TABLE_EXISTENT                  , "01000" },
{ FK_NOT_TABLE                    , "01000" },
{ PK_ONLYONE                      , "01000" },
{ KEY_DUPLICATE                   , "01000" },
{ MONTH_TOOLARGE                  , "01000" },
{ DAYS_TOOLARGE                   , "01000" },
{ HOURS_TOOLARGE                  , "01000" },
{ MINUTES_TOOLARGE                , "01000" },
{ SECS_TOOLARGE                   , "01000" },
{ MILLIS_TOOLARGE                 , "01000" },
{ DATETIME_INVALID                , "01000" },
{ UNSUPPORTED_CONVERSION_OPER     , "01000" },
{ UNSUPPORTED_DATATYPE_OPER       , "01000" },
{ UNSUPPORTED_DATATYPE_FUNC       , "01000" },
{ UNSUPPORTED_CONVERSION_FUNC     , "01000" },
{ UNSUPPORTED_TYPE_CONV           , "01000" },
{ UNSUPPORTED_TYPE_SUM            , "01000" },
{ UNSUPPORTED_TYPE_MAX            , "01000" },
{ UNSUPPORTED_CONVERSION          , "01000" },
{ INSERT_INVALID_LEN              , "01000" },
{ SUBSTR_INVALID_LEN              , "01000" },
{ VALUE_STR_TOOLARGE              , "01000" },
{ VALUE_BIN_TOOLARGE              , "01000" },
{ VALUE_NULL_INVALID              , "01000" },
{ VALUE_CANT_CONVERT              , "01000" },
{ BYTEARR_INVALID_SIZE            , "01000" },
{ LOB_DELETED                     , "01000" },
{ PARAM_CLASS_UNKNOWN             , "01000" },
{ PARAM_EMPTY                     , "01000" },
{ PARAM_IDX_OUT_RANGE             , "01000" },
{ COL_DUPLICATE                	  , "01000" },
{ COL_MISSING                     , "01000" },
{ COL_VAL_UNMATCH                 , "01000" },
{ COL_INVALID_SIZE                , "01000" },
{ COL_WRONG_PREFIX                , "01000" },
{ COL_READONLY                    , "01000" },
{ COL_INVALID_NAME                , "01000" },
{ COL_IDX_OUT_RANGE               , "01000" },
{ COL_AMBIGUOUS                   , "01000" },
{ GROUP_AGGR_INVALID              , "01000" },
{ GROUP_AGGR_NOTPART              , "01000" },
{ ORDERBY_INTERNAL                , "01000" },
{ UNION_DIFFERENT_COLS            , "01000" },
{ INDEX_EXISTS                    , "01000" },
{ INDEX_MISSING                   , "01000" },
{ INDEX_FILE_INVALID              , "01000" },
{ INDEX_CORRUPT                   , "01000" },
{ INDEX_TOOMANY_EQUALS            , "01000" },
{ FILE_TOONEW                     , "01000" },
{ FILE_TOOOLD                     , "01000" },
{ FILE_CANT_DELETE                , "01000" },
{ ROW_0_ABSOLUTE                  , "01000" },
{ ROW_NOCURRENT                   , "01000" },
{ ROWS_WRONG_MAX                  , "01000" },
{ ROW_LOCKED                      , "01000" },
{ ROW_DELETED                     , "01000" },
{ SAVEPT_INVALID_TRANS            , "01000" },
{ SAVEPT_INVALID_DRIVER           , "01000" },
{ ALIAS_UNSUPPORTED               , "01000" },
{ ISOLATION_UNKNOWN               , "01000" },
{ FLAGVALUE_INVALID               , "01000" },
{ ARGUMENT_INVALID                , "01000" },
{ GENER_KEYS_UNREQUIRED           , "01000" },
{ SEQUENCE_HEX_INVALID            , "01000" },
{ SEQUENCE_HEX_INVALID_STR        , "01000" },
{ SYNTAX_BASE_OFS            	  , "01000" },
{ SYNTAX_BASE_END        		  , "01000" },
{ STXADD_ADDITIONAL_TOK			  , "01000" },
{ STXADD_IDENT_EXPECT			  , "01000" },
{ STXADD_IDENT_EMPTY 			  , "01000" },
{ STXADD_IDENT_WRONG 			  , "01000" },
{ STXADD_OPER_MINUS 			  , "01000" },
{ STXADD_FUNC_UNKNOWN 			  , "01000" },
{ STXADD_PARAM_INVALID_COUNT	  , "01000" },
{ STXADD_JOIN_INVALID	  		  , "01000" },
{ STXADD_FROM_PAR_CLOSE	  		  , "01000" },
{ STXADD_KEYS_REQUIRED	  		  , "01000" },
{ STXADD_NOT_NUMBER	  		      , "01000" },
{ STXADD_COMMENT_OPEN			  , "01000" },
};
}
package smallsql.database;
import java.sql.*;
import java.util.ArrayList;
import smallsql.database.language.Language;
class SSStatement implements Statement{
final SSConnection con;
Command cmd;
private boolean isClosed;
int rsType;
int rsConcurrency;
private int fetchDirection;
private int fetchSize;
private int queryTimeout;
private int maxRows;
private int maxFieldSize;
private ArrayList batches;
private boolean needGeneratedKeys;
private ResultSet generatedKeys;
private int[] generatedKeyIndexes;
private String[] generatedKeyNames;
SSStatement(SSConnection con) throws SQLException{
this(con, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
}
SSStatement(SSConnection con, int rsType, int rsConcurrency) throws SQLException{
this.con = con;
this.rsType = rsType;
this.rsConcurrency = rsConcurrency;
con.testClosedConnection();
}
final public ResultSet executeQuery(String sql) throws SQLException{
executeImpl(sql);
return cmd.getQueryResult();
}
final public int executeUpdate(String sql) throws SQLException{
executeImpl(sql);
return cmd.getUpdateCount();
}
final public boolean execute(String sql) throws SQLException{
executeImpl(sql);
return cmd.getResultSet() != null;
}
final private void executeImpl(String sql) throws SQLException{
checkStatement();
generatedKeys = null;
try{
con.log.println(sql);
SQLParser parser = new SQLParser();
cmd = parser.parse(con, sql);
if(maxRows != 0 && (cmd.getMaxRows() == -1 || cmd.getMaxRows() > maxRows))
cmd.setMaxRows(maxRows);
cmd.execute(con, this);
}catch(Exception e){
throw SmallSQLException.createFromException(e);
}
needGeneratedKeys = false;
generatedKeyIndexes = null;
generatedKeyNames = null;
}
final public void close(){
con.log.println("Statement.close");
isClosed = true;
cmd = null;
}
final public int getMaxFieldSize(){
return maxFieldSize;
}
final public void setMaxFieldSize(int max){
maxFieldSize = max;
}
final public int getMaxRows(){
return maxRows;
}
final public void setMaxRows(int max) throws SQLException{
if(max < 0)
throw SmallSQLException.create(Language.ROWS_WRONG_MAX, String.valueOf(max));
maxRows = max;
}
final public void setEscapeProcessing(boolean enable) throws SQLException{
checkStatement();
}
final public int getQueryTimeout() throws SQLException{
checkStatement();
return queryTimeout;
}
final public void setQueryTimeout(int seconds) throws SQLException{
checkStatement();
queryTimeout = seconds;
}
final public void cancel() throws SQLException{
checkStatement();
}
final public SQLWarning getWarnings(){
return null;
}
final public void clearWarnings(){
}
final public void setCursorName(String name) throws SQLException{
final void setGeneratedKeys(ResultSet rs){
generatedKeys = rs;
}
final public ResultSet getGeneratedKeys() throws SQLException{
if(generatedKeys == null)
throw SmallSQLException.create(Language.GENER_KEYS_UNREQUIRED);
return generatedKeys;
}
final public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
setNeedGeneratedKeys(autoGeneratedKeys);
return executeUpdate(sql);
}
final public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
setNeedGeneratedKeys(columnIndexes);
return executeUpdate(sql);
}
final public int executeUpdate(String sql, String[] columnNames) throws SQLException{
setNeedGeneratedKeys(columnNames);
return executeUpdate(sql);
}
final public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
setNeedGeneratedKeys(autoGeneratedKeys);
return execute(sql);
}
final public boolean execute(String sql, int[] columnIndexes) throws SQLException{
setNeedGeneratedKeys(columnIndexes);
return execute(sql);
}
final public boolean execute(String sql, String[] columnNames) throws SQLException{
setNeedGeneratedKeys(columnNames);
return execute(sql);
}
final public int getResultSetHoldability() throws SQLException{
package smallsql.database.language;
public class Language_it extends Language {
protected Language_it() {
addMessages(ENTRIES);
}
public String[][] getEntries() {
return ENTRIES;
}
private final String[][] ENTRIES = {
{ UNSUPPORTED_OPERATION           , "Operazione non supportata: {0}." },
{ CANT_LOCK_FILE                  , "Impossibile bloccare il file ''{0}''. Un database SmallSQL Database può essere aperto da un unico processo." },
{ DB_EXISTENT                     , "Il database ''{0}'' è già esistente." },
{ DB_NONEXISTENT                  , "Il database ''{0}'' Non esiste." },
{ DB_NOT_DIRECTORY                , "La directory ''{0}'' non è un database SmallSQL." },
{ DB_NOTCONNECTED                 , "L''utente non è connesso a un database." },
{ CONNECTION_CLOSED               , "La connessione è già chiusa." },
{ VIEW_INSERT                     , "INSERT non è supportato per una view." },
{ VIEWDROP_NOT_VIEW               , "Non è possibile effettuare DROP VIEW con ''{0}'' perché non è una view." },
{ VIEW_CANTDROP                   , "Non si può effettuare drop sulla view ''{0}''." },
{ RSET_NOT_PRODUCED               , "Nessun ResultSet è stato prodotto." },
{ RSET_READONLY                   , "Il ResultSet è di sola lettura." },
{ RSET_FWDONLY                    , "Il ResultSet è forward only." }, 
{ RSET_CLOSED                     , "Il ResultSet è chiuso." },
{ RSET_NOT_INSERT_ROW             , "Il cursore non è attualmente nella riga ''InsertRow''." },
{ RSET_ON_INSERT_ROW              , "Il cursore è attualmente nella riga ''InsertRow''." },
{ ROWSOURCE_READONLY              , "Il Rowsource è di sola lettura." },
{ STMT_IS_CLOSED                  , "Lo Statement è in stato chiuso." },
{ SUBQUERY_COL_COUNT              , "Il conteggio delle colonne nella subquery deve essere 1 e non {0}." },
{ JOIN_DELETE                     , "DeleteRow non supportato nelle join." },
{ JOIN_INSERT                     , "InsertRow non supportato nelle join." },
{ DELETE_WO_FROM                  , "DeleteRow necessita un''espressione FROM." },
{ INSERT_WO_FROM                  , "InsertRow necessita un''espressione FROM." },
{ TABLE_CANT_RENAME               , "La tabella ''{0}'' non può essere rinominata." },
{ TABLE_CANT_DROP                 , "Non si può effettuare DROP della tabella ''{0}''." },
{ TABLE_CANT_DROP_LOCKED          , "Non si può effettuare DROP della tabella ''{0}'' perché è in LOCK." },
{ TABLE_CORRUPT_PAGE              , "Pagina della tabella corrotta alla posizione: {0}." },
{ TABLE_MODIFIED                  , "La tabella ''{0}'' è stata modificata." },
{ TABLE_DEADLOCK                  , "Deadlock: non si può mettere un lock sulla tabella ''{0}''." },
{ TABLE_OR_VIEW_MISSING           , "La tabella/view ''{0}'' non esiste." },
{ TABLE_FILE_INVALID              , "Il file ''{0}'' non include una tabella SmallSQL valida." },
{ TABLE_OR_VIEW_FILE_INVALID      , "Il file ''{0}'' non è un contenitore valido di tabella/view." },
{ TABLE_EXISTENT                  , "La tabella/vista ''{0}'' è già esistente." },
{ FK_NOT_TABLE                    , "''{0}'' non è una tabella." },
{ PK_ONLYONE                      , "Una tabella può avere solo una primary key." },
{ KEY_DUPLICATE                   , "Chiave duplicata." },
{ MONTH_TOOLARGE                  , "Valore del mese troppo alto del in DATE o TIMESTAMP ''{0}''." },
{ DAYS_TOOLARGE                   , "Valore del giorno troppo altro in DATE o TIMESTAMP ''{0}''." },
{ HOURS_TOOLARGE                  , "Valore delle ore troppo alto in in TIME o TIMESTAMP ''{0}''." },
{ MINUTES_TOOLARGE                , "Valore dei minuti troppo alto in TIME o TIMESTAMP ''{0}''." },
{ SECS_TOOLARGE                   , "Valore dei secondi troppo alto in TIME o TIMESTAMP ''{0}''." },
{ MILLIS_TOOLARGE                 , "VAlore dei millisecondi troppo alto in TIMESTAMP ''{0}''." },
{ DATETIME_INVALID                , "''{0}'' è un DATE, TIME or TIMESTAMP non valido." },
{ UNSUPPORTED_CONVERSION_OPER     , "Conversione non supportata verso il tipo di dato ''{0}'' dal tipo ''{1}'' per l''operazione ''{2}''." },
{ UNSUPPORTED_DATATYPE_OPER       , "Tipo di dato ''{0}'' non supportato per l''operazione ''{1}''." },
{ UNSUPPORTED_DATATYPE_FUNC       , "Tipo di dato ''{0}'' non supportato per la funzione ''{1}''." },
{ UNSUPPORTED_CONVERSION_FUNC     , "Conversione verso il tipo di dato ''{0}'' non supportato per la funzione ''{1}''." },
{ UNSUPPORTED_TYPE_CONV           , "Tipo non supportato per la funzione CONVERT: {0}." },
{ UNSUPPORTED_TYPE_SUM            , "Tipo non supportato per la funzione SUM: ''{0}''." },
{ UNSUPPORTED_TYPE_MAX            , "Tipo non supportato per la funzione MAX: ''{0}''." },
{ UNSUPPORTED_CONVERSION          , "Non è possible convertire ''{0}'' [{1}] in ''{2}''." },
{ INSERT_INVALID_LEN              , "Lunghezza non valida ''{0}'' per la funzione INSERT." },
{ SUBSTR_INVALID_LEN              , "Lunghezza non valida ''{0}'' per la funzione SUBSTRING." },
{ VALUE_STR_TOOLARGE              , "Stringa troppo lunga per la colonna." },
{ VALUE_BIN_TOOLARGE              , "Valore binario di lunghezza {0} eccessiva per la colonna di lunghezza {1}." },
{ VALUE_NULL_INVALID              , "Valori nulli non validi per la colonna ''{0}''." },
{ VALUE_CANT_CONVERT              , "Impossible convertire un valore {0} in un valore {1}." },
{ BYTEARR_INVALID_SIZE            , "Lunghezza non valida per un array di bytes: {0}." },
{ LOB_DELETED                     , "L''oggetto LOB è stato cancellato." },
{ PARAM_CLASS_UNKNOWN             , "Classe sconosciuta (''{0}'') per il parametro." },
{ PARAM_EMPTY                     , "Il parametro {0} è vuoto." },
{ PARAM_IDX_OUT_RANGE             , "L''indice {0} per il parametro è fuori dall''intervallo consentito ( 1 <= n <= {1} )." },
{ COL_DUPLICATE                	  , "Nome di colonna duplicato: ''{0}''." },
{ COL_MISSING                     , "Colonna ''{0}'' non trovata." },
{ COL_VAL_UNMATCH                 , "Il conteggio di colonne e valori non è identico." },
{ COL_INVALID_SIZE                , "Lunghezza non valida ({0}) per la colonna ''{1}''." },
{ COL_WRONG_PREFIX                , "Il prefisso di colonna ''{0}'' non coincide con un alias o nome di tabella usato nella query." },
{ COL_READONLY                    , "La colonna ''{0}'' è di sola lettura." },
{ COL_INVALID_NAME                , "Nome di colonna non valido ''{0}''." },
{ COL_IDX_OUT_RANGE               , "Indice di colonna fuori dall''intervallo valido: {0}." },
{ COL_AMBIGUOUS                   , "Il nome di colonna ''{0}'' è ambiguo." },
{ GROUP_AGGR_INVALID              , "Funzione di aggregrazione non valida per la clausola GROUP BY: ({0})." },
{ GROUP_AGGR_NOTPART              , "L''espressione ''{0}'' non è parte di una funzione di aggregazione o della clausola GROUP BY." },
{ ORDERBY_INTERNAL                , "Errore interno per ORDER BY." },
{ UNION_DIFFERENT_COLS            , "SELECT appartenenti ad una UNION con numero di colonne differenti: {0} e {1}." },
{ INDEX_EXISTS                    , "L''indice ''{0}'' è già esistente." },
{ INDEX_MISSING                   , "L''indice ''{0}'' non esiste." },
{ INDEX_FILE_INVALID              , "Il file ''{0}'' non è un contenitore valido per un indice." },
{ INDEX_CORRUPT                   , "Errore durante il caricamento dell''indice. File dell''indice corrotto: ''{0}''." },
{ INDEX_TOOMANY_EQUALS            , "Troppe voci uguali nell''indice." },
{ FILE_TOONEW                     , "La versione ({0}) del file ''{1}'' è troppo recente per questo runtime." },
{ FILE_TOOOLD                     , "La versione ({0}) del file ''{1}'' è troppo vecchia per questo runtime." },
{ FILE_CANT_DELETE                , "File ''(0)'' non possono essere eliminati." },
{ ROW_0_ABSOLUTE                  , "Il numero di riga 0 non è valido per il metodo ''absolute()''." },
{ ROW_NOCURRENT                   , "Nessuna riga corrente." },
{ ROWS_WRONG_MAX                  , "Numero massimo di righe non valido ({0})." },
{ ROW_LOCKED                      , "La riga è bloccata da un''altra connessione." },
{ ROW_DELETED                     , "Riga già cancellata." },
{ SAVEPT_INVALID_TRANS            , "SAVEPOINT non valido per questa transazione." },
{ SAVEPT_INVALID_DRIVER           , "SAVEPOINT non valido per questo driver {0}." },
{ ALIAS_UNSUPPORTED               , "Alias non supportato per questo tipo di sorgente righe." },
{ ISOLATION_UNKNOWN               , "Livello di Isolamento transazione sconosciuto: {0}." },
{ FLAGVALUE_INVALID               , "Flag non valida nel metodo ''getMoreResults'': {0}." },
{ ARGUMENT_INVALID                , "Argomento non valido nel metodo ''setNeedGenratedKeys'': {0}." },
{ GENER_KEYS_UNREQUIRED           , "GeneratedKeys non richieste." },
{ SEQUENCE_HEX_INVALID            , "Sequenza esadecimale non valido alla posizione {0}." },
{ SEQUENCE_HEX_INVALID_STR        , "Sequence esadecimale non valida alla positione {0} in ''{1}''." },
{ SYNTAX_BASE_OFS            	  , "Errore di sintassi alla posizione {0} in ''{1}''. " },
{ SYNTAX_BASE_END        		  , "Errore di sintassi, fine inattesa della stringa SQL. " },
{ STXADD_ADDITIONAL_TOK			  , "Token aggiuntivo dopo la fine dell''istruzione SQL." },
{ STXADD_IDENT_EXPECT			  , "Identificatore atteso." },
{ STXADD_IDENT_EMPTY 			  , "Identificatore vuoto." },
{ STXADD_IDENT_WRONG 			  , "Identificatore errato ''{0}''." },
{ STXADD_OPER_MINUS 			  , "Operatore ''meno'' non valido per il tipo di dato varbinary." },
{ STXADD_FUNC_UNKNOWN 			  , "Funzione sconosciuta." },
{ STXADD_PARAM_INVALID_COUNT	  , "Totale parametri non valido." },
{ STXADD_JOIN_INVALID	  		  , "Sintassi della join non valida." },
{ STXADD_FROM_PAR_CLOSE	  		  , "Parentesi chiusa non attesa nella clausola from." },
{ STXADD_KEYS_REQUIRED	  		  , "Le parole chiave richieste sono: " },
{ STXADD_NOT_NUMBER	  		      , "Richiesto valore numerico (passato = ''{0}'')." },
{ STXADD_COMMENT_OPEN	  		  , "Chiusura del commento mancante (''*/'')." },
};
}
package smallsql.database;
import smallsql.database.language.Language;
final class SortedResult extends RowSource {
final private Expressions orderBy;
final private RowSource rowSource;
private IndexScrollStatus scrollStatus;
private int row;
private final LongList insertedRows = new LongList();
private boolean useSetRowPosition;
private int sortedRowCount;
private long lastRowOffset;
SortedResult(RowSource rowSource, Expressions orderBy){
this.rowSource = rowSource;
this.orderBy = orderBy;
}
final boolean isScrollable(){
return true;
}
final void execute() throws Exception{
rowSource.execute();
Index index = new Index(false);
lastRowOffset = -1;
while(rowSource.next()){
lastRowOffset = rowSource.getRowPosition();
index.addValues( lastRowOffset, orderBy);
sortedRowCount++;
}
scrollStatus = index.createScrollStatus(orderBy);
useSetRowPosition = false;
}
final boolean isBeforeFirst(){
return row == 0;
}
final boolean isFirst(){
return row == 1;
}
void beforeFirst() throws Exception {
scrollStatus.reset();
row = 0;
useSetRowPosition = false;
}
boolean first() throws Exception {
beforeFirst();
return next();
}
boolean previous() throws Exception{
if(useSetRowPosition) throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
if(currentInsertedRow() == 0){
scrollStatus.afterLast();
}
row--;
if(currentInsertedRow() >= 0){
rowSource.setRowPosition( insertedRows.get( currentInsertedRow() ) );
return true;
}
long rowPosition = scrollStatus.getRowOffset(false);
if(rowPosition >= 0){
rowSource.setRowPosition( rowPosition );
return true;
}else{
rowSource.noRow();
row = 0;
return false;
}
}
boolean next() throws Exception {
if(useSetRowPosition) throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
if(currentInsertedRow() < 0){
long rowPosition = scrollStatus.getRowOffset(true);
if(rowPosition >= 0){
row++;
rowSource.setRowPosition( rowPosition );
return true;
}
}
if(currentInsertedRow() < insertedRows.size()-1){
row++;
rowSource.setRowPosition( insertedRows.get( currentInsertedRow() ) );
return true;
}
if(lastRowOffset >= 0){
rowSource.setRowPosition( lastRowOffset );
}else{
rowSource.beforeFirst();
}
if(rowSource.next()){
row++;
lastRowOffset = rowSource.getRowPosition();
insertedRows.add( lastRowOffset );
return true;
}
rowSource.noRow();
row = (getRowCount() > 0) ? getRowCount() + 1 : 0;
return false;
}
boolean last() throws Exception{
afterLast();
return previous();
}
final boolean isLast() throws Exception{
if(row == 0){
return false;
}
if(row > getRowCount()){
return false;
}
boolean isNext = next();
previous();
return !isNext;
}
final boolean isAfterLast(){
int rowCount = getRowCount();
return row > rowCount || rowCount == 0;
}
void afterLast() throws Exception{
useSetRowPosition = false;
if(sortedRowCount > 0){
scrollStatus.afterLast();
scrollStatus.getRowOffset(false); 
}else{
rowSource.beforeFirst();
}
row = sortedRowCount;
while(next()){
}
}
boolean absolute(int newRow) throws Exception{
if(newRow == 0) throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
if(newRow > 0){
beforeFirst();
while(newRow-- > 0){
if(!next()){
return false;
}
}
}else{
afterLast();
while(newRow++ < 0){
if(!previous()){
return false;
}
}
}
return true;
}
boolean relative(int rows) throws Exception{
if(rows == 0) return (row != 0);
if(rows > 0){
while(rows-- > 0){
if(!next()){
return false;
}
}
}else{
while(rows++ < 0){
if(!previous()){
return false;
}
}
}
return true;
}
int getRow(){
return row > getRowCount() ? 0 : row;
}
final long getRowPosition(){
return rowSource.getRowPosition();
}
final void setRowPosition(long rowPosition) throws Exception{
rowSource.setRowPosition(rowPosition);
useSetRowPosition = true;
}
final boolean rowInserted(){
return rowSource.rowInserted();
}
final boolean rowDeleted(){
return rowSource.rowDeleted();
}
void nullRow() {
rowSource.nullRow();
row = 0;
}
void noRow() {
rowSource.noRow();
row = 0;
}
boolean isExpressionsFromThisRowSource(Expressions columns){
return rowSource.isExpressionsFromThisRowSource(columns);
}
private final int getRowCount(){
return sortedRowCount + insertedRows.size();
}
private final int currentInsertedRow(){
return row - sortedRowCount - 1;
}
}
package smallsql.junit;
import java.sql.*;
public class TestScrollable extends BasicTestCase {
public void testLastWithWhere() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
assertRowCount( 0, "Select * from Scrollable");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
assertRowCount( 1, "Select * from Scrollable");
assertRowCount( 0, "Select * from Scrollable Where 1=0");
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
testLastWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
testLastWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
testLastWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Order By v") );
testLastWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
}finally{
dropTable( con, "Scrollable");
}
}
private void testLastWithWhereAssert(ResultSet rs) throws Exception{
assertFalse( "There should be no rows:", rs.last());
assertFalse( "isLast", rs.isLast());
try{
rs.getString("v");
fail("SQLException 'No current row' should be throw");
}catch(SQLException ex){
assertSQLException( "01000", 0, ex );
}
}
public void testNextWithWhere() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
assertRowCount( 0, "Select * from Scrollable");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
assertRowCount( 1, "Select * from Scrollable");
assertRowCount( 0, "Select * from Scrollable Where 1=0");
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
testNextWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
testNextWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
testNextWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v") );
testNextWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
}finally{
dropTable( con, "Scrollable");
}
}
private void testNextWithWhereAssert(ResultSet rs) throws Exception{
assertFalse("There should be no rows:", rs.next());
try{
rs.getString("v");
fail("SQLException 'No current row' should be throw");
}catch(SQLException ex){
assertSQLException( "01000", 0, ex);
}
}
public void testFirstWithWhere() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
assertRowCount( 0, "Select * from Scrollable");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
assertRowCount( 1, "Select * from Scrollable");
assertRowCount( 0, "Select * from Scrollable Where 1=0");
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
testFirstWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
testFirstWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
testFirstWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v") );
testFirstWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
}finally{
dropTable( con, "Scrollable");
}
}
private void testFirstWithWhereAssert(ResultSet rs) throws Exception{
assertFalse( "isFirst", rs.isFirst() );
assertTrue( rs.isBeforeFirst() );
assertFalse( "There should be no rows:", rs.first());
assertFalse( "isFirst", rs.isFirst() );
assertTrue( rs.isBeforeFirst() );
try{
rs.getString("v");
fail("SQLException 'No current row' should be throw");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
}
public void testPreviousWithWhere() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
assertRowCount( 0, "Select * from Scrollable");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
assertRowCount( 1, "Select * from Scrollable");
assertRowCount( 0, "Select * from Scrollable Where 1=0");
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
testPreviousWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
testPreviousWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
testPreviousWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v") );
testPreviousWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
testPreviousWithWhereAssert( st.executeQuery("Select v from Scrollable Group By v Having 1=0 Order By v") );
}finally{
dropTable( con, "Scrollable");
}
}
private void testPreviousWithWhereAssert(ResultSet rs) throws Exception{
assertTrue( rs.isBeforeFirst() );
assertTrue( rs.isAfterLast() );
rs.afterLast();
assertTrue( rs.isAfterLast() );
assertFalse("There should be no rows:", rs.previous());
try{
rs.getString("v");
fail("SQLException 'No current row' should be throw");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
}
public void testAbsoluteRelative() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
assertRowCount( 0, "Select * from Scrollable");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert1')");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert2')");
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert3')");
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
testAbsoluteRelativeAssert( st.executeQuery("Select * from Scrollable") );
testAbsoluteRelativeAssert( st.executeQuery("Select * from Scrollable Order By i") );
testAbsoluteRelativeAssert( st.executeQuery("Select v from Scrollable Group By v") );
testAbsoluteRelativeAssert( st.executeQuery("Select v from Scrollable Group By v Order By v") );
}finally{
dropTable( con, "Scrollable");
}
}
private void testAbsoluteRelativeAssert(ResultSet rs) throws SQLException{
assertEquals(0, rs.getRow());
assertTrue(rs.absolute(2));
assertEquals("qwert2", rs.getString("v"));
assertEquals(2, rs.getRow());
assertTrue(rs.relative(-1));
assertEquals("qwert1", rs.getString("v"));
assertEquals(1, rs.getRow());
assertTrue(rs.absolute(1));
assertEquals("qwert1", rs.getString("v"));
assertEquals(1, rs.getRow());
assertTrue(rs.isFirst());
assertTrue(rs.relative(1));
assertEquals("qwert2", rs.getString("v"));
assertEquals(2, rs.getRow());
assertFalse(rs.isLast());
assertFalse(rs.isFirst());
assertTrue(rs.absolute(-1));
assertEquals("qwert3", rs.getString("v"));
assertEquals(3, rs.getRow());
assertTrue(rs.isLast());
assertFalse(rs.isFirst());
assertTrue(rs.relative(0));
assertEquals("qwert3", rs.getString("v"));
assertEquals(3, rs.getRow());
assertTrue(rs.isLast());
assertFalse(rs.isFirst());
assertFalse(rs.absolute(4));
assertEquals(0, rs.getRow());
assertFalse(rs.isLast());
assertFalse(rs.isFirst());
assertFalse(rs.isBeforeFirst());
assertTrue(rs.isAfterLast());
assertTrue(rs.last());
assertEquals(3, rs.getRow());
assertTrue(rs.isLast());
assertFalse(rs.isFirst());
assertFalse(rs.absolute(-4));
assertEquals(0, rs.getRow());
assertFalse(rs.isLast());
assertFalse(rs.isFirst());
assertTrue(rs.isBeforeFirst());
assertFalse(rs.isAfterLast());
assertFalse(rs.relative(4));
assertEquals(0, rs.getRow());
assertFalse(rs.isLast());
assertFalse(rs.isFirst());
assertFalse(rs.isBeforeFirst());
assertTrue(rs.isAfterLast());
assertFalse(rs.relative(-4));
assertEquals(0, rs.getRow());
assertFalse(rs.isLast());
assertFalse(rs.isFirst());
assertTrue(rs.isBeforeFirst());
assertFalse(rs.isAfterLast());
}
public void testUpdatable() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Scrollable (i int Identity primary key, v varchar(20))");
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
for(int row = 1; row < 4; row++){
testUpdatableAssert( con, st.executeQuery("Select * from Scrollable"), row );
testUpdatableAssert( con, st.executeQuery("Select * from Scrollable Order By i"), row );
testUpdatableAssert( con, st.executeQuery("Select * from Scrollable Where 1 = 1"), row );
testUpdatableAssert( con, st.executeQuery("Select * from Scrollable Where 1 = 1 Order By i"), row );
con.createStatement().execute("Insert Into Scrollable(v) Values('qwert" +row + "')");
}
}finally{
dropTable( con, "Scrollable");
}
}
private void testUpdatableAssert( Connection con, ResultSet rs, int row) throws Exception{
con.setAutoCommit(false);
for(int r=row; r < 4; r++){
rs.moveToInsertRow();
rs.updateString( "v", "qwert" + r);
rs.insertRow();
}
assertTrue( rs.last() );
assertEquals( 3, rs.getRow() );
rs.beforeFirst();
assertRowCount( 3, rs );
rs.beforeFirst();
testAbsoluteRelativeAssert(rs);
con.rollback();
assertRowCount( row - 1, con.createStatement().executeQuery("Select * from Scrollable"));
rs.last();
assertTrue( rs.rowDeleted() );
assertTrue( rs.rowInserted() );
rs.beforeFirst();
assertRowCount( 3, rs );
con.setAutoCommit(true);
}
}
package smallsql.database;
final class DataSources {
private int size;
private DataSource[] data = new DataSource[4];
final int size(){
return size;
}
final DataSource get(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
return data[idx];
}
final void add(DataSource table){
if(size >= data.length ){
DataSource[] dataNew = new DataSource[size << 1];
System.arraycopy(data, 0, dataNew, 0, size);
data = dataNew;
}
data[size++] = table;
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import smallsql.database.language.Language;
class Table extends TableView{
private static final int INDEX = 1;
final Database database;
FileChannel raFile; 
private Lobs lobs; 
long firstPage; 
final private HashMap locks = new HashMap();
private SSConnection tabLockConnection; 
private int tabLockCount;
Table( Database database, SSConnection con, String name, FileChannel raFile, long offset, int tableFormatVersion) throws Exception{
super( name, new Columns() );
this.database = database;
this.raFile   = raFile;
this.firstPage = offset;
StoreImpl store = getStore(con, firstPage, SQLTokenizer.SELECT);
if(store == null){
throw SmallSQLException.create(Language.TABLE_FILE_INVALID, getFile(database));
}
int count = store.readInt();
for(int i=0; i<count; i++){
columns.add( store.readColumn(tableFormatVersion) );
}
indexes = new IndexDescriptions();
references = new ForeignKeys();
int type;
while((type = store.readInt()) != 0){
int offsetInPage = store.getCurrentOffsetInPage();
int size = store.readInt();
switch(type){
case INDEX:
indexes.add( IndexDescription.load( database, this, store) );
break;
}
store.setCurrentOffsetInPage(offsetInPage + size);
}
firstPage = store.getNextPagePos();
}
Table(Database database, SSConnection con, String name, Columns columns, IndexDescriptions indexes, ForeignKeys foreignKeys) throws Exception{
this(database, con, name, columns, null, indexes, foreignKeys);
}
Table(Database database, SSConnection con, String name, Columns columns, IndexDescriptions existIndexes, IndexDescriptions newIndexes, ForeignKeys foreignKeys) throws Exception{
super( name, columns );
this.database = database;
this.references = foreignKeys;
newIndexes.create(con, database, this);
if(existIndexes == null){
this.indexes = newIndexes;
}else{
this.indexes = existIndexes;
existIndexes.add(newIndexes);
}
write(con);
for(int i=0; i<foreignKeys.size(); i++){
ForeignKey foreignKey = foreignKeys.get(i);
Table pkTable = (Table)database.getTableView(con, foreignKey.pkTable);
pkTable.references.add(foreignKey);
}
}
Table(Database database, String name){
super( name, null);
this.database = database;
indexes = null;
references = null;
}
static void drop(Database database, String name) throws Exception{
boolean ok = new File( Utils.createTableViewFileName( database, name ) ).delete();
if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
}
void drop(SSConnection con) throws Exception{
TableStorePage storePage = requestLock( con, SQLTokenizer.CREATE, -1 );
if(storePage == null){
throw SmallSQLException.create(Language.TABLE_CANT_DROP_LOCKED, name);
}
con.rollbackFile(raFile);
close();
if(lobs != null)
lobs.drop(con);
if(indexes != null)
indexes.drop(database);
boolean ok = getFile(database).delete();
if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
}
@Override
void close() throws Exception{
if(indexes != null)
indexes.close();
raFile.close();
raFile = null;
if( lobs != null ){
lobs.close();
lobs = null;
}
}
private void write(SSConnection con) throws Exception{
raFile = createFile( con, database );
firstPage = 8;
StoreImpl store = getStore( con, firstPage, SQLTokenizer.CREATE);
int count = columns.size();
store.writeInt( count );
for(int i=0; i<count; i++){
store.writeColumn(columns.get(i));
}
for(int i=0; i<indexes.size(); i++){
IndexDescription indexDesc = indexes.get(i);
store.writeInt( INDEX );
int offsetStart = store.getCurrentOffsetInPage();
store.setCurrentOffsetInPage( offsetStart + 4 ); 
indexDesc.save(store);
int offsetEnd = store.getCurrentOffsetInPage();
store.setCurrentOffsetInPage( offsetStart );
store.writeInt( offsetEnd - offsetStart);
store.setCurrentOffsetInPage( offsetEnd );
}
store.writeInt( 0 ); 
store.writeFinsh(null); 
firstPage = store.getNextPagePos();
}
@Override
void writeMagic(FileChannel raFile) throws Exception{
ByteBuffer buffer = ByteBuffer.allocate(8);
buffer.putInt(MAGIC_TABLE);
buffer.putInt(TABLE_VIEW_VERSION);
buffer.position(0);
raFile.write(buffer);
}
StoreImpl getStore( SSConnection con, long filePos, int pageOperation ) throws Exception{
TableStorePage storePage = requestLock( con, pageOperation, filePos );
return StoreImpl.createStore( this, storePage, pageOperation, filePos );
}
StoreImpl getStore( TableStorePage storePage, int pageOperation ) throws Exception{
return StoreImpl.recreateStore( this, storePage, pageOperation );
}
StoreImpl getStoreInsert( SSConnection con ) throws Exception{
TableStorePage storePage = requestLock( con, SQLTokenizer.INSERT, -1 );
return StoreImpl.createStore( this, storePage, SQLTokenizer.INSERT, -1 );
}
StoreImpl getStoreTemp( SSConnection con ) throws Exception{
TableStorePage storePage = new TableStorePage( con, this, LOCK_NONE, -2);
return StoreImpl.createStore( this, storePage, SQLTokenizer.INSERT, -2 );
}
StoreImpl getLobStore(SSConnection con, long filePos, int pageOperation) throws Exception{
if(lobs == null){
lobs = new Lobs( this );
}
return lobs.getStore( con, filePos, pageOperation );
}
final long getFirstPage(){
return firstPage;
}
List getInserts(SSConnection con){
synchronized(locks){
ArrayList inserts = new ArrayList();
if(con.isolationLevel <= Connection.TRANSACTION_READ_UNCOMMITTED){
for(int i=0; i<locksInsert.size(); i++){
TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
inserts.add(lock.getLink());
}
}else{
for(int i=0; i<locksInsert.size(); i++){
TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
if(lock.con == con)
inserts.add(lock.getLink());
}
}
return inserts;
}
}
final TableStorePage requestLock(SSConnection con, int pageOperation, long page) throws Exception{
synchronized(locks){
if(raFile == null){
throw SmallSQLException.create(Language.TABLE_MODIFIED, name);
}
long endTime = 0;
while(true){
TableStorePage storePage = requestLockImpl( con, pageOperation, page);
if(storePage != null)
return storePage; 
if(endTime == 0)
endTime = System.currentTimeMillis() + 5000;
long waitTime = endTime - System.currentTimeMillis();
if(waitTime <= 0)
throw SmallSQLException.create(Language.TABLE_DEADLOCK, name);
locks.wait(waitTime);
}
}
}
final private TableStorePage requestLockImpl(SSConnection con, int pageOperation, long page) throws SQLException{
synchronized(locks){
if(tabLockConnection != null && tabLockConnection != con) return null;
switch(con.isolationLevel){
case Connection.TRANSACTION_SERIALIZABLE:
serializeConnections.put( con, con);
break;
}
switch(pageOperation){
case SQLTokenizer.CREATE:{
if(locks.size() > 0){
Iterator values = locks.values().iterator();
while(values.hasNext()){
TableStorePage lock = (TableStorePage)values.next();
if(lock.con != con) return null;
}
}
for(int i=0; i<locksInsert.size(); i++){
TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
if(lock.con != con) return null;
}
if(serializeConnections.size() > 0){
Iterator values = locks.values().iterator();
while(values.hasNext()){
TableStorePage lock = (TableStorePage)values.next();
if(lock.con != con) return null;
}
}
tabLockConnection = con;
tabLockCount++;
TableStorePage lock = new TableStorePage(con, this, LOCK_TAB, page);
con.add(lock);
return lock;
}
case SQLTokenizer.ALTER:{
if(locks.size() > 0 || locksInsert.size() > 0){
return null;
}
if(serializeConnections.size() > 0){
Iterator values = locks.values().iterator();
while(values.hasNext()){
TableStorePage lock = (TableStorePage)values.next();
if(lock.con != con) return null;
}
}
tabLockConnection = con;
tabLockCount++;
TableStorePage lock = new TableStorePage(con, this, LOCK_TAB, page);
lock.rollback();
return lock;
}
case SQLTokenizer.INSERT:{
if(serializeConnections.size() > 1) return null;
if(serializeConnections.size() == 1 && serializeConnections.get(con) == null) return null;
TableStorePageInsert lock = new TableStorePageInsert(con, this, LOCK_INSERT);
locksInsert.add( lock );
con.add(lock);
return lock;
}
case SQLTokenizer.SELECT:
case SQLTokenizer.UPDATE:{
Long pageKey = new Long(page); 
TableStorePage prevLock = null;
TableStorePage lock = (TableStorePage)locks.get( pageKey );
TableStorePage usableLock = null;
while(lock != null){
if(lock.con == con ||
con.isolationLevel <= Connection.TRANSACTION_READ_UNCOMMITTED){
usableLock = lock;
} else {
if(lock.lockType == LOCK_WRITE){
return null; 
}
}
prevLock = lock;
lock = lock.nextLock;
}
if(usableLock != null){
return usableLock;
}
lock = new TableStorePage( con, this, LOCK_NONE, page);
if(con.isolationLevel >= Connection.TRANSACTION_REPEATABLE_READ || pageOperation == SQLTokenizer.UPDATE){
lock.lockType = pageOperation == SQLTokenizer.UPDATE ? LOCK_WRITE : LOCK_READ;
if(prevLock != null){
prevLock.nextLock = lock.nextLock;
}else{
locks.put( pageKey, lock );
}
con.add(lock);
}
return lock;
}
case SQLTokenizer.LONGVARBINARY:
return new TableStorePage( con, this, LOCK_INSERT, -1);
default:
throw new Error("pageOperation:"+pageOperation);
}
}
}
TableStorePage requestWriteLock(SSConnection con, TableStorePage readlock) throws SQLException{
if(readlock.lockType == LOCK_INSERT){
TableStorePage lock = new TableStorePage( con, this, LOCK_INSERT, -1);
readlock.nextLock = lock;
con.add(lock);
return lock;
}
Long pageKey = new Long(readlock.fileOffset); 
TableStorePage prevLock = null;
TableStorePage lock = (TableStorePage)locks.get( pageKey );
while(lock != null){
if(lock.con != con) return null; 
if(lock.lockType < LOCK_WRITE){
lock.lockType = LOCK_WRITE;
return lock;
}
prevLock = lock;
lock = lock.nextLock;
}
lock = new TableStorePage( con, this, LOCK_WRITE, readlock.fileOffset);
if(prevLock != null){
prevLock.nextLock = lock;
} else {
locks.put( pageKey, lock );
}
con.add(lock);
return lock;
}
void freeLock(TableStorePage storePage){
final int lockType = storePage.lockType;
final long fileOffset = storePage.fileOffset;
synchronized(locks){
try{
TableStorePage lock;
TableStorePage prev;
switch(lockType){
case LOCK_INSERT:
for(int i=0; i<locksInsert.size(); i++){
prev = lock = (TableStorePage)locksInsert.get(i);
while(lock != null){
if(lock == storePage){
if(lock == prev){
if(lock.nextLock == null){
locksInsert.remove(i--);
}else{
locksInsert.set( i, lock.nextLock );
}
}else{
prev.nextLock = lock.nextLock;
}
return;
}
prev = lock;
lock = lock.nextLock;
}
}
break;
case LOCK_READ:
case LOCK_WRITE:
Long pageKey = new Long(fileOffset); 
lock = (TableStorePage)locks.get( pageKey );
prev = lock;
while(lock != null){
if(lock == storePage){
if(lock == prev){
if(lock.nextLock == null){
locks.remove(pageKey);
}else{
locks.put( pageKey, lock.nextLock );
}
}else{
prev.nextLock = lock.nextLock;
}
return;
}
prev = lock;
lock = lock.nextLock;
}
break;
case LOCK_TAB:
assert storePage.con == tabLockConnection : "Internal Error with TabLock";
if(--tabLockCount == 0) tabLockConnection = null;
break;
default:
throw new Error();
}
}finally{
locks.notifyAll();
}
}
}
}
package smallsql.database;
class LongList {
private int size;
private long[] data;
LongList(){
this(16);
}
LongList(int initialSize){
data = new long[initialSize];
}
final int size(){
return size;
}
final long get(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
return data[idx];
}
final void add(long value){
if(size >= data.length ){
resize(size << 1);
}
data[ size++ ] = value;
}
final void clear(){
size = 0;
}
private final void resize(int newSize){
long[] dataNew = new long[newSize];
System.arraycopy(data, 0, dataNew, 0, size);
data = dataNew;
}
}
package smallsql.database.language;
public class Language_en extends Language{
}
package smallsql.database;
public class LongTreeListEnum {
long[] resultStack = new long[4];
int[]  offsetStack = new int[4];
int stack;
final void reset(){
stack = 0;
offsetStack[0] = 0;
}
}
package smallsql.database;
final class ExpressionFunctionExp extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.EXP; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.exp( param1.getDouble() );
}
}
package smallsql.database;
final class ExpressionFunctionCeiling extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.CEILING; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.ceil( param1.getDouble() );
}
}
package smallsql.database;
import java.util.List;
import java.sql.*;
import smallsql.database.language.Language;
final class SQLParser {
SSConnection con;
private char[] sql;
private List tokens;
private int tokenIdx;
Command parse(SSConnection con, String sqlString) throws SQLException{
this.con = con;
Command cmd = parse( sqlString.toCharArray() );
SQLToken token = nextToken();
if(token != null){
throw createSyntaxError(token, Language.STXADD_ADDITIONAL_TOK);
}
return cmd;
}
final private Command parse(char[] sql) throws SQLException{
this.sql = sql;
this.tokens = SQLTokenizer.parseSQL( sql );
tokenIdx = 0;
SQLToken token = nextToken(COMMANDS);
switch (token.value){
case SQLTokenizer.SELECT:
return select();
case SQLTokenizer.DELETE:
return delete();
case SQLTokenizer.INSERT:
return insert();
case SQLTokenizer.UPDATE:
return update();
case SQLTokenizer.CREATE:
return create();
case SQLTokenizer.DROP:
return drop();
case SQLTokenizer.ALTER:
return alter();
case SQLTokenizer.SET:
return set();
case SQLTokenizer.USE:
token = nextToken(MISSING_EXPRESSION);
String name = token.getName( sql );
checkValidIdentifier( name, token );
CommandSet set = new CommandSet( con.log, SQLTokenizer.USE);
set.name = name;
return set;
case SQLTokenizer.EXECUTE:
return execute();
case SQLTokenizer.TRUNCATE:
return truncate();
default:
throw new Error();
}
}
Expression parseExpression(String expr) throws SQLException{
this.sql = expr.toCharArray();
this.tokens = SQLTokenizer.parseSQL( sql );
tokenIdx = 0;
return expression( null, 0);
}
private SQLException createSyntaxError(SQLToken token, String addMessageCode) {
String message = getErrorString(token, addMessageCode, null);
return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
}
private SQLException createSyntaxError(SQLToken token, String addMessageCode,
Object param0) {
String message = getErrorString(token, addMessageCode, param0);
return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
}
private SQLException createSyntaxError(SQLToken token, int[] validValues){
String msgStr = SmallSQLException.translateMsg(
Language.STXADD_KEYS_REQUIRED, new Object[] { });
StringBuffer msgBuf = new StringBuffer( msgStr );
for(int i=0; i<validValues.length; i++){
String name = SQLTokenizer.getKeyWord(validValues[i]);
if(name == null) name = String.valueOf( (char)validValues[i] );
msgBuf.append( name );
if (i < validValues.length - 2)
msgBuf.append( ", ");
else
if ( i == validValues.length - 2 )
msgBuf.append( " or ");
}
String message = getErrorString(
token, Language.CUSTOM_MESSAGE, msgBuf);
return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
}
private String getErrorString(SQLToken token, String middleMsgCode,
Object middleMsgParam) {
StringBuffer buffer = new StringBuffer(1024);
String middle = SmallSQLException.translateMsg(
middleMsgCode, new Object[] { middleMsgParam });
buffer.append(middle);
private String getIdentifier(SQLToken token) throws SQLException{
String name = token.getName(sql);
checkValidIdentifier( name, token );
return name;
}
private String nextIdentifier() throws SQLException{
return getIdentifier( nextToken( MISSING_IDENTIFIER ) );
}
private String nextIdentiferPart(String name) throws SQLException{
SQLToken token = nextToken();
if(token != null && token.value == SQLTokenizer.POINT){
return nextIdentifier();
}else{
previousToken();
}
return name;
}
final private boolean isKeyword(SQLToken token){
if(token == null) return false;
switch(token.value){
case SQLTokenizer.SELECT:
case SQLTokenizer.INSERT:
case SQLTokenizer.UPDATE:
case SQLTokenizer.UNION:
case SQLTokenizer.FROM:
case SQLTokenizer.WHERE:
case SQLTokenizer.GROUP:
case SQLTokenizer.HAVING:
case SQLTokenizer.ORDER:
case SQLTokenizer.COMMA:
case SQLTokenizer.SET:
case SQLTokenizer.JOIN:
case SQLTokenizer.LIMIT:
return true;
}
return false;
}
private SQLToken lastToken(){
if(tokenIdx > tokens.size()){
return null;
}
return (SQLToken)tokens.get( tokenIdx-1 );
}
private void previousToken(){
tokenIdx--;
}
private SQLToken nextToken(){
if(tokenIdx >= tokens.size()){
tokenIdx++; 
return null;
}
return (SQLToken)tokens.get( tokenIdx++ );
}
private SQLToken nextToken( int[] validValues) throws SQLException{
SQLToken token = nextToken();
if(token == null) throw createSyntaxError( token, validValues);
if(validValues == MISSING_EXPRESSION){
return token; 
}
if(validValues == MISSING_IDENTIFIER){
switch(token.value){
case SQLTokenizer.PARENTHESIS_L:
case SQLTokenizer.PARENTHESIS_R:
case SQLTokenizer.COMMA:
throw createSyntaxError( token, validValues);
}
return token;
}
for(int i=validValues.length-1; i>=0; i--){
if(token.value == validValues[i]) return token;
}
throw createSyntaxError( token, validValues);
}
private CommandSelect singleSelect() throws SQLException{
CommandSelect selCmd = new CommandSelect(con.log);
SQLToken token;
Switch: while(true){
token = nextToken(MISSING_EXPRESSION);
switch(token.value){
case SQLTokenizer.TOP:
token = nextToken(MISSING_EXPRESSION);
try{
int maxRows = Integer.parseInt(token.getName(sql));
selCmd.setMaxRows(maxRows);
}catch(NumberFormatException e){
throw createSyntaxError(token, Language.STXADD_NOT_NUMBER, token.getName(sql));
}
break;
case SQLTokenizer.ALL:
selCmd.setDistinct(false);
break;
case SQLTokenizer.DISTINCT:
selCmd.setDistinct(true);
break;
default:
previousToken();
break Switch;
}
}
while(true){
Expression column = expression(selCmd, 0);
selCmd.addColumnExpression( column );
token = nextToken();
if(token == null) return selCmd; 
boolean as = false;
if(token.value == SQLTokenizer.AS){
token = nextToken(MISSING_EXPRESSION);
as = true;
}
if(as || (!isKeyword(token))){
String alias = getIdentifier( token);
column.setAlias( alias );
token = nextToken();
if(token == null) return selCmd; 
}
switch(token.value){
case SQLTokenizer.COMMA:
if(column == null) throw createSyntaxError( token, MISSING_EXPRESSION );
column = null;
break;
case SQLTokenizer.FROM:
if(column == null) throw createSyntaxError( token, MISSING_EXPRESSION );
column = null;
from(selCmd);
return selCmd;
default:
if(!isKeyword(token))
throw createSyntaxError( token, new int[]{SQLTokenizer.COMMA, SQLTokenizer.FROM} );
previousToken();
return selCmd;
}
}
}
final private CommandSelect select() throws SQLException{
CommandSelect selCmd = singleSelect();
SQLToken token = nextToken();
UnionAll union = null;
while(token != null && token.value == SQLTokenizer.UNION){
if(union == null){
union = new UnionAll();
union.addDataSource(new ViewResult( con, selCmd ));
selCmd = new CommandSelect(con.log);
selCmd.setSource( union );
DataSources from = new DataSources();
from.add(union);
selCmd.setTables( from );
selCmd.addColumnExpression( new ExpressionName("*") );
}
nextToken(MISSING_ALL);
nextToken(MISSING_SELECT);
union.addDataSource( new ViewResult( con, singleSelect() ) );
token = nextToken();
}
if(token != null && token.value == SQLTokenizer.ORDER){
order( selCmd );
token = nextToken();
}
if(token != null && token.value == SQLTokenizer.LIMIT){
limit( selCmd );
token = nextToken();
}
previousToken();
return selCmd;
}
private Command delete() throws SQLException{
CommandDelete cmd = new CommandDelete(con.log);
nextToken(MISSING_FROM);
from(cmd);
SQLToken token = nextToken();
if(token != null){
if(token.value != SQLTokenizer.WHERE)
throw this.createSyntaxError(token, MISSING_WHERE);
where(cmd);
}
return cmd;
}
private Command truncate() throws SQLException{
CommandDelete cmd = new CommandDelete(con.log);
nextToken(MISSING_TABLE);
from(cmd);
return cmd;
}
private Command insert() throws SQLException{
SQLToken token = nextToken( MISSING_INTO );
CommandInsert cmd = new CommandInsert( con.log, nextIdentifier() );
int parthesisCount = 0;
token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
if(token.value == SQLTokenizer.PARENTHESIS_L){
token = nextToken(MISSING_EXPRESSION);
if(token.value == SQLTokenizer.SELECT){
parthesisCount++;
cmd.noColumns = true;
}else{
previousToken();
Expressions list = expressionParenthesisList(cmd);
for(int i=0; i<list.size(); i++){
cmd.addColumnExpression( list.get( i ) );
}
token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
}
}else cmd.noColumns = true;
Switch: while(true)
switch(token.value){
case SQLTokenizer.VALUES:{
token = nextToken(MISSING_PARENTHESIS_L);
cmd.addValues( expressionParenthesisList(cmd) );
return cmd;
}
case SQLTokenizer.SELECT:
cmd.addValues( select() );
while(parthesisCount-- > 0){
nextToken(MISSING_PARENTHESIS_R);
}
return cmd;
case SQLTokenizer.PARENTHESIS_L:
token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
parthesisCount++;
continue Switch;
default:
throw new Error();
}
}
private Command update() throws SQLException{
CommandUpdate cmd = new CommandUpdate(con.log);
DataSources tables = new DataSources();
cmd.setTables( tables );
cmd.setSource( rowSource( cmd, tables, 0 ) );
SQLToken token = nextToken(MISSING_SET);
while(true){
token = nextToken();
Expression dest = expressionSingle( cmd, token);
if(dest.getType() != Expression.NAME) throw createSyntaxError( token, MISSING_IDENTIFIER );
nextToken(MISSING_EQUALS);
Expression src = expression(cmd, 0);
cmd.addSetting( dest, src);
token = nextToken();
if(token == null) break;
switch(token.value){
case SQLTokenizer.WHERE:
where(cmd);
return cmd;
case SQLTokenizer.COMMA:
continue;
default: throw createSyntaxError( token, MISSING_WHERE_COMMA );
}
}
return cmd;
}
private Command create() throws SQLException{
while(true){
SQLToken token = nextToken(COMMANDS_CREATE);
switch(token.value){
case SQLTokenizer.DATABASE:
return createDatabase();
case SQLTokenizer.TABLE:
return createTable();
case SQLTokenizer.VIEW:
return createView();
case SQLTokenizer.INDEX:
return createIndex(false);
case SQLTokenizer.PROCEDURE:
return createProcedure();
case SQLTokenizer.UNIQUE:
do{
token = nextToken(COMMANDS_CREATE_UNIQUE);
}while(token.value == SQLTokenizer.INDEX);
return createIndex(true);
case SQLTokenizer.NONCLUSTERED:
case SQLTokenizer.CLUSTERED:
continue;
default:
throw createSyntaxError( token, COMMANDS_CREATE );
}
}
}
private CommandCreateDatabase createDatabase() throws SQLException{
SQLToken token = nextToken();
if(token == null) throw createSyntaxError( token, MISSING_EXPRESSION );
return new CommandCreateDatabase( con.log, token.getName(sql));
}
private CommandTable createTable() throws SQLException{
String catalog;
String tableName = catalog = nextIdentifier();
tableName = nextIdentiferPart(tableName);
if(tableName == catalog) catalog = null;
CommandTable cmdCreate = new CommandTable( con.log, catalog, tableName, SQLTokenizer.CREATE );
SQLToken token = nextToken( MISSING_PARENTHESIS_L );
nextCol:
while(true){
token = nextToken( MISSING_EXPRESSION );
String constraintName;
if(token.value == SQLTokenizer.CONSTRAINT){
constraintName = nextIdentifier();
token = nextToken( MISSING_KEYTYPE );
}else{
constraintName = null;
}
switch(token.value){
case SQLTokenizer.PRIMARY:
case SQLTokenizer.UNIQUE:
case SQLTokenizer.FOREIGN:
IndexDescription index = index(cmdCreate, token.value, tableName, constraintName, null);
if(token.value == SQLTokenizer.FOREIGN){
nextToken( MISSING_REFERENCES );
String pk = nextIdentifier();
Expressions expressions = new Expressions();
Strings columns = new Strings();
expressionDefList( cmdCreate, expressions, columns );
IndexDescription pkIndex = new IndexDescription( null, pk, SQLTokenizer.UNIQUE, expressions, columns);
ForeignKey foreignKey = new ForeignKey(pk, pkIndex, tableName, index);
cmdCreate.addForeingnKey(foreignKey);
}else{
cmdCreate.addIndex( index );
}
token = nextToken( MISSING_COMMA_PARENTHESIS );
switch(token.value){
case SQLTokenizer.PARENTHESIS_R:
return cmdCreate;
case SQLTokenizer.COMMA:
continue nextCol;
}
}
token = addColumn( token, cmdCreate );
if(token == null){
throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
}
switch(token.value){
case SQLTokenizer.PARENTHESIS_R:
return cmdCreate;
case SQLTokenizer.COMMA:
continue nextCol;
default:
throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
}
}
}
private SQLToken addColumn(SQLToken token, CommandTable cmdCreate) throws SQLException{
String colName = getIdentifier( token );
Column col = datatype(false);
col.setName( colName );
token = nextToken();
boolean nullableWasSet = false;
boolean defaultWasSet = col.isAutoIncrement(); 
while(true){
if(token == null){
cmdCreate.addColumn( col );
return null;
}
switch(token.value){
case SQLTokenizer.PARENTHESIS_R:
case SQLTokenizer.COMMA:
cmdCreate.addColumn( col );
return token;
case SQLTokenizer.DEFAULT:
if(defaultWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
int offset = token.offset + token.length;
token = nextToken();
if(token != null) offset = token.offset;
previousToken();
Expression expr = expression(cmdCreate, 0);
SQLToken last = lastToken();
int length = last.offset + last.length - offset;
String def = new String( sql, offset, length );
col.setDefaultValue( expr, def );
defaultWasSet = true;
break;
case SQLTokenizer.IDENTITY:
if(defaultWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
col.setAutoIncrement(true);
defaultWasSet = true;
break;
case SQLTokenizer.NULL:
if(nullableWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
nullableWasSet = true;
break;
case SQLTokenizer.NOT:
if(nullableWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
token = nextToken( MISSING_NULL );
col.setNullable(false);
nullableWasSet = true;
break;
case SQLTokenizer.PRIMARY:
case SQLTokenizer.UNIQUE:
IndexDescription index = index(cmdCreate, token.value, cmdCreate.name, null, colName);
cmdCreate.addIndex( index );
break;
default:
throw createSyntaxError(token, MISSING_OPTIONS_DATATYPE);
}
token = nextToken();
}
}
private IndexDescription index(Command cmd, int constraintType, String tableName, String contrainName, String columnName) throws SQLException{
if(constraintType != SQLTokenizer.UNIQUE) nextToken( MISSING_KEY );
SQLToken token = nextToken();
if(token != null){
switch(token.value){
case SQLTokenizer.CLUSTERED:
case SQLTokenizer.NONCLUSTERED:
break;
default:
previousToken();
}
}else{
previousToken();
}
Strings columns = new Strings();
Expressions expressions = new Expressions();
if(columnName != null){
columns.add(columnName);
expressions.add(new ExpressionName(columnName));
}else{
expressionDefList( cmd, expressions, columns );
}
return new IndexDescription( contrainName, tableName, constraintType, expressions, columns);
}
private Column datatype(boolean isEscape) throws SQLException{
SQLToken token;
int dataType;
if(isEscape){
token = nextToken( MISSING_SQL_DATATYPE );
switch(token.value){
case SQLTokenizer.SQL_BIGINT: 			dataType = SQLTokenizer.BIGINT;		break;
case SQLTokenizer.SQL_BINARY:			dataType = SQLTokenizer.BINARY; 	break;
case SQLTokenizer.SQL_BIT:				dataType = SQLTokenizer.BIT;		break;
case SQLTokenizer.SQL_CHAR:				dataType = SQLTokenizer.CHAR;		break;
case SQLTokenizer.SQL_DATE:				dataType = SQLTokenizer.DATE;		break;
case SQLTokenizer.SQL_DECIMAL:			dataType = SQLTokenizer.DECIMAL;	break;
case SQLTokenizer.SQL_DOUBLE:			dataType = SQLTokenizer.DOUBLE;		break;
case SQLTokenizer.SQL_FLOAT:			dataType = SQLTokenizer.FLOAT;		break;
case SQLTokenizer.SQL_INTEGER:			dataType = SQLTokenizer.INT;		break;
case SQLTokenizer.SQL_LONGVARBINARY:	dataType = SQLTokenizer.LONGVARBINARY;break;
case SQLTokenizer.SQL_LONGVARCHAR:		dataType = SQLTokenizer.LONGVARCHAR;break;
case SQLTokenizer.SQL_REAL:				dataType = SQLTokenizer.REAL;		break;
case SQLTokenizer.SQL_SMALLINT:			dataType = SQLTokenizer.SMALLINT;	break;
case SQLTokenizer.SQL_TIME:				dataType = SQLTokenizer.TIME;		break;
case SQLTokenizer.SQL_TIMESTAMP:		dataType = SQLTokenizer.TIMESTAMP;	break;
case SQLTokenizer.SQL_TINYINT:			dataType = SQLTokenizer.TINYINT;	break;
case SQLTokenizer.SQL_VARBINARY:		dataType = SQLTokenizer.VARBINARY;	break;
case SQLTokenizer.SQL_VARCHAR:			dataType = SQLTokenizer.VARCHAR;	break;
default: throw new Error();
}
}else{
token = nextToken( MISSING_DATATYPE );
dataType = token.value;
}
Column col = new Column();
if(dataType == SQLTokenizer.LONG){
token = nextToken();
if(token != null && token.value == SQLTokenizer.RAW){
dataType = SQLTokenizer.LONGVARBINARY;
}else{
dataType = SQLTokenizer.LONGVARCHAR;
previousToken();
}
}
switch(dataType){
case SQLTokenizer.RAW:
dataType = SQLTokenizer.VARBINARY;
case SQLTokenizer.CHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
{
token = nextToken();
int displaySize;
if(token == null || token.value != SQLTokenizer.PARENTHESIS_L){
displaySize = 30;
previousToken();
}else{
token = nextToken( MISSING_EXPRESSION );
try{
displaySize = Integer.parseInt(token.getName(sql) );
}catch(Exception e){
throw createSyntaxError(token, MISSING_NUMBERVALUE );
}
nextToken( MISSING_PARENTHESIS_R );
}
col.setPrecision( displaySize );
break;
}
case SQLTokenizer.SYSNAME:
col.setPrecision(255);
dataType = SQLTokenizer.VARCHAR;
break;
case SQLTokenizer.COUNTER:
col.setAutoIncrement(true);
dataType = SQLTokenizer.INT;
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
token = nextToken();
if(token != null && token.value == SQLTokenizer.PARENTHESIS_L){
token = nextToken( MISSING_EXPRESSION );
int value;
try{
value = Integer.parseInt(token.getName(sql) );
}catch(Exception e){
throw createSyntaxError(token, MISSING_NUMBERVALUE );
}
col.setPrecision(value);
token = nextToken( MISSING_COMMA_PARENTHESIS );
if(token.value == SQLTokenizer.COMMA){
token = nextToken( MISSING_EXPRESSION );
try{
value = Integer.parseInt(token.getName(sql) );
}catch(Exception e){
throw createSyntaxError(token, MISSING_NUMBERVALUE );
}
col.setScale(value);
nextToken( MISSING_PARENTHESIS_R );
}
}else{
col.setPrecision(18); 
previousToken();
}
break;
}
col.setDataType( dataType );
return col;
}
private CommandCreateView createView() throws SQLException{
String viewName = nextIdentifier();
nextToken(MISSING_AS);
SQLToken token = nextToken(MISSING_SELECT);
CommandCreateView cmd = new CommandCreateView( con.log, viewName );
cmd.sql = new String(sql, token.offset, sql.length-token.offset );
select(); 
return cmd;
}
private CommandTable createIndex(boolean unique) throws SQLException{
String indexName = nextIdentifier();
nextToken(MISSING_ON);
String catalog;
String tableName = catalog = nextIdentifier();
tableName = nextIdentiferPart(tableName);
if(tableName == catalog) catalog = null;
CommandTable cmd = new CommandTable( con.log, catalog, tableName, SQLTokenizer.INDEX );
Expressions expressions = new Expressions();
Strings columns = new Strings();
expressionDefList( cmd, expressions, columns );
IndexDescription indexDesc = new IndexDescription(
indexName,
tableName,
unique ? SQLTokenizer.UNIQUE : SQLTokenizer.INDEX,
expressions,
columns);
Object[] param = { "Create Index" };
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
}
private CommandCreateDatabase createProcedure() throws SQLException{
Object[] param = { "Create Procedure" };
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
}
private Command drop() throws SQLException{
SQLToken tokenType = nextToken(COMMANDS_DROP);
String catalog;
String name = catalog = nextIdentifier();
name = nextIdentiferPart( name );
if(name == catalog) catalog = null;
switch(tokenType.value){
case SQLTokenizer.DATABASE:
case SQLTokenizer.TABLE:
case SQLTokenizer.VIEW:
case SQLTokenizer.INDEX:
case SQLTokenizer.PROCEDURE:
return new CommandDrop( con.log, catalog, name, tokenType.value);
default:
throw createSyntaxError( tokenType, COMMANDS_DROP );
}
}
private Command alter() throws SQLException{
SQLToken tokenType = nextToken(COMMANDS_ALTER);
String catalog;
String tableName = catalog = nextIdentifier();
switch(tokenType.value){
case SQLTokenizer.TABLE:
case SQLTokenizer.VIEW:
case SQLTokenizer.INDEX:
case SQLTokenizer.PROCEDURE:
tableName = nextIdentiferPart(tableName);
if(tableName == catalog) catalog = null;
}
switch(tokenType.value){
case SQLTokenizer.TABLE:
return alterTable( catalog, tableName );
default:
Object[] param = { "ALTER " + tokenType.getName( sql ) };
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
}
}
Command alterTable( String catalog, String name ) throws SQLException{
SQLToken tokenType = nextToken(MISSING_ADD_ALTER_DROP);
CommandTable cmd = new CommandTable( con.log, catalog, name, tokenType.value );
switch(tokenType.value){
case SQLTokenizer.ADD:
SQLToken token;
do{
token = nextToken( MISSING_IDENTIFIER );
token = addColumn( token, cmd );
}while(token != null && token.value == SQLTokenizer.COMMA );
return cmd;
default:
Object[] param = { "ALTER TABLE " + tokenType.getName( sql ) };
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
}
}
private CommandSet set() throws SQLException{
SQLToken token = nextToken( COMMANDS_SET );
switch(token.value){
case SQLTokenizer.TRANSACTION:
return setTransaction();
default:
throw new Error();
}
}
private CommandSet setTransaction() throws SQLException{
SQLToken token = nextToken( MISSING_ISOLATION );
token = nextToken( MISSING_LEVEL );
token = nextToken( COMMANDS_TRANS_LEVEL );
CommandSet cmd = new CommandSet( con.log, SQLTokenizer.LEVEL );
switch(token.value){
case SQLTokenizer.READ:
token = nextToken( MISSING_COMM_UNCOMM );
switch(token.value){
case SQLTokenizer.COMMITTED:
cmd.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
break;
case SQLTokenizer.UNCOMMITTED:
cmd.isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
break;
default:
throw new Error();
}
return cmd;
case SQLTokenizer.REPEATABLE:
token = nextToken( MISSING_READ );
cmd.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
return cmd;
case SQLTokenizer.SERIALIZABLE:
cmd.isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
return cmd;
default:
throw new Error();
}
}
private Command execute() throws SQLException{
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Execute");
}
private Expressions expressionParenthesisList(Command cmd) throws SQLException{
Expressions list = new Expressions();
{
SQLToken token = nextToken();
if(token != null && token.value == SQLTokenizer.PARENTHESIS_R){
return list;
}
previousToken();
}
while(true){
list.add( expression(cmd, 0) );
SQLToken token = nextToken(MISSING_COMMA_PARENTHESIS);
switch(token.value){
case SQLTokenizer.PARENTHESIS_R:
return list;
case SQLTokenizer.COMMA:
continue;
default:
throw new Error();
}
}
}
private Expressions expressionTokenList(Command cmd, int listType) throws SQLException{
Expressions list = new Expressions();
while(true){
Expression expr = expression(cmd, 0);
list.add( expr );
SQLToken token = nextToken();
if(listType == SQLTokenizer.ORDER && token != null){
switch(token.value){
case SQLTokenizer.DESC:
expr.setAlias(SQLTokenizer.DESC_STR);
case SQLTokenizer.ASC:
token = nextToken();
}
}
if(token == null) {
previousToken();
return list;
}
switch(token.value){
case SQLTokenizer.COMMA:
continue;
default:
if(isKeyword(token) ){
previousToken();
return list;
}
throw createSyntaxError( token, MISSING_TOKEN_LIST);
}
}
}
private void expressionDefList(Command cmd, Expressions expressions, Strings columns) throws SQLException{
SQLToken token = nextToken();
if(token.value != SQLTokenizer.PARENTHESIS_L) throw createSyntaxError(token, MISSING_PARENTHESIS_L );
Loop:
while(true){
int offset = token.offset + token.length;
token = nextToken();
if(token != null) offset = token.offset;
previousToken();
expressions.add( expression(cmd, 0) );
SQLToken last = lastToken();
int length = last.offset + last.length - offset;
columns.add( new String( sql, offset, length ) );
token = nextToken(MISSING_COMMA_PARENTHESIS);
switch(token.value){
case SQLTokenizer.PARENTHESIS_R:
break Loop;
case SQLTokenizer.COMMA:
continue;
default:
throw new Error();
}
}
}
private Expression expression(Command cmd, int previousOperationLevel) throws SQLException{
SQLToken token = nextToken(MISSING_EXPRESSION);
Expression leftExpr;
switch(token.value){
case SQLTokenizer.NOT:
leftExpr =  new ExpressionArithmetic( expression( cmd, ExpressionArithmetic.NOT      / 10), ExpressionArithmetic.NOT);
break;
case SQLTokenizer.MINUS:
leftExpr =  new ExpressionArithmetic( expression( cmd, ExpressionArithmetic.NEGATIVE / 10), ExpressionArithmetic.NEGATIVE);
break;
case SQLTokenizer.TILDE:
leftExpr =  new ExpressionArithmetic( expression( cmd, ExpressionArithmetic.BIT_NOT  / 10), ExpressionArithmetic.BIT_NOT);
break;
case SQLTokenizer.PARENTHESIS_L:
leftExpr = expression( cmd, 0);
token = nextToken(MISSING_PARENTHESIS_R);
break;
default:
leftExpr = expressionSingle( cmd, token);
}
boolean isNot = false;
while((token = nextToken()) != null){
Expression rightExpr;
int operation = ExpressionArithmetic.getOperationFromToken(token.value);
int level = operation / 10;
if(previousOperationLevel >= level){
previousToken();
return leftExpr;
}
switch(token.value){
case SQLTokenizer.PLUS:
case SQLTokenizer.MINUS:
case SQLTokenizer.ASTERISK:
case SQLTokenizer.SLACH:
case SQLTokenizer.PERCENT:
case SQLTokenizer.EQUALS:
case SQLTokenizer.LESSER:
case SQLTokenizer.LESSER_EQU:
case SQLTokenizer.GREATER:
case SQLTokenizer.GREATER_EQU:
case SQLTokenizer.UNEQUALS:
case SQLTokenizer.LIKE:
case SQLTokenizer.OR:
case SQLTokenizer.AND:
case SQLTokenizer.BIT_AND:
case SQLTokenizer.BIT_OR:
case SQLTokenizer.BIT_XOR:
rightExpr = expression( cmd, level );
leftExpr = new ExpressionArithmetic( leftExpr, rightExpr, operation );
break;
case SQLTokenizer.BETWEEN:
rightExpr = expression( cmd, ExpressionArithmetic.AND );
nextToken( MISSING_AND );
Expression rightExpr2 = expression( cmd, level );
leftExpr = new ExpressionArithmetic( leftExpr, rightExpr, rightExpr2, operation );
break;
case SQLTokenizer.IN:
nextToken(MISSING_PARENTHESIS_L);
token = nextToken(MISSING_EXPRESSION);
if(token.value == SQLTokenizer.SELECT){
CommandSelect cmdSel = select();
leftExpr = new ExpressionInSelect( con, leftExpr, cmdSel, operation );
nextToken(MISSING_PARENTHESIS_R);
}else{
previousToken();
Expressions list = expressionParenthesisList( cmd );
leftExpr = new ExpressionArithmetic( leftExpr, list, operation );
}
break;
case SQLTokenizer.IS:
token = nextToken(MISSING_NOT_NULL);
if(token.value == SQLTokenizer.NOT){
nextToken(MISSING_NULL);
operation++;
}
leftExpr = new ExpressionArithmetic( leftExpr, operation );
break;
case SQLTokenizer.NOT:
token = nextToken(MISSING_BETWEEN_IN);
previousToken();
isNot = true;
continue;
default:
previousToken();
return leftExpr;
}
if(isNot){
isNot = false;
leftExpr =  new ExpressionArithmetic( leftExpr, ExpressionArithmetic.NOT);
}
}
previousToken();
return leftExpr;
}
private Expression expressionSingle(Command cmd, SQLToken token) throws SQLException{
boolean isMinus = false;
if(token != null){
switch(token.value){
case SQLTokenizer.NULL:
return new ExpressionValue( null, SQLTokenizer.NULL );
case SQLTokenizer.STRING:
return new ExpressionValue( token.getName(null), SQLTokenizer.VARCHAR );
case SQLTokenizer.IDENTIFIER:
{
String name = getIdentifier( token );
ExpressionName expr =  new ExpressionName( name );
SQLToken token2 = nextToken();
if(token2 != null && token2.value == SQLTokenizer.POINT){
expr.setNameAfterTableAlias( nextIdentifier() );
}else{
previousToken();
}
return expr;
}
case SQLTokenizer.TRUE:
return new ExpressionValue( Boolean.TRUE, SQLTokenizer.BOOLEAN );
case SQLTokenizer.FALSE:
return new ExpressionValue( Boolean.FALSE, SQLTokenizer.BOOLEAN );
case SQLTokenizer.ESCAPE_L:{
token = nextToken(COMMANDS_ESCAPE);
SQLToken para = nextToken(MISSING_EXPRESSION);
Expression expr;
switch(token.value){
case SQLTokenizer.D: 
expr = new ExpressionValue( DateTime.valueOf(para.getName(sql), SQLTokenizer.DATE), SQLTokenizer.DATE );
break;
case SQLTokenizer.T: 
expr = new ExpressionValue( DateTime.valueOf(para.getName(sql), SQLTokenizer.TIME), SQLTokenizer.TIME );
break;
case SQLTokenizer.TS: 
expr = new ExpressionValue( DateTime.valueOf(para.getName(sql), SQLTokenizer.TIMESTAMP), SQLTokenizer.TIMESTAMP );
break;
case SQLTokenizer.FN: 
nextToken(MISSING_PARENTHESIS_L);
expr = function(cmd, para, true);
break;
case SQLTokenizer.CALL: 
throw new java.lang.UnsupportedOperationException("call escape sequence");
default: throw new Error();
}
token = nextToken( ESCAPE_MISSING_CLOSE );
return expr;
}
case SQLTokenizer.QUESTION:
ExpressionValue param = new ExpressionValue();
cmd.addParameter( param );
return param;
case SQLTokenizer.CASE:
return caseExpr(cmd);
case SQLTokenizer.MINUS:
case SQLTokenizer.PLUS:
do{
if(token.value == SQLTokenizer.MINUS)
isMinus = !isMinus;
token = nextToken();
if(token == null) throw createSyntaxError( token, MISSING_EXPRESSION );
}while(token.value == SQLTokenizer.MINUS || token.value == SQLTokenizer.PLUS);
default:
SQLToken token2 = nextToken();
if(token2 != null && token2.value == SQLTokenizer.PARENTHESIS_L){
if(isMinus)
return new ExpressionArithmetic( function( cmd, token, false ),  ExpressionArithmetic.NEGATIVE );
return function( cmd, token, false );
}else{
char chr1 = sql[ token.offset ];
if(chr1 == '$'){
previousToken();
String tok = new String(sql, token.offset+1, token.length-1);
if(isMinus) tok = "-" + tok;
return new ExpressionValue( new Money(Double.parseDouble(tok)), SQLTokenizer.MONEY );
}
String tok = new String(sql, token.offset, token.length);
if((chr1 >= '0' && '9' >= chr1) || chr1 == '.'){
previousToken();
if(token.length>1 && (sql[ token.offset +1 ] | 0x20) == 'x'){
if(isMinus) {
throw createSyntaxError(token, Language.STXADD_OPER_MINUS);
}
return new ExpressionValue( Utils.hex2bytes( sql, token.offset+2, token.length-2), SQLTokenizer.VARBINARY );
}
if(isMinus) tok = "-" + tok;
if(Utils.indexOf( '.', sql, token.offset, token.length ) >= 0 ||
Utils.indexOf( 'e', sql, token.offset, token.length ) >= 0){
return new ExpressionValue( new Double(tok), SQLTokenizer.DOUBLE );
}else{
try{
return new ExpressionValue( new Integer(tok), SQLTokenizer.INT );
}catch(NumberFormatException e){
return new ExpressionValue( new Long(tok), SQLTokenizer.BIGINT );
}
}
}else{
checkValidIdentifier( tok, token );
ExpressionName expr = new ExpressionName(tok);
if(token2 != null && token2.value == SQLTokenizer.POINT){
expr.setNameAfterTableAlias( nextIdentifier() );
}else{
previousToken();
}
if(isMinus)
return new ExpressionArithmetic( expr,  ExpressionArithmetic.NEGATIVE );
return expr;
}
}
}
}
return null;
}
ExpressionFunctionCase caseExpr(final Command cmd) throws SQLException{
ExpressionFunctionCase expr = new ExpressionFunctionCase();
SQLToken token = nextToken(MISSING_EXPRESSION);
Expression input = null;
if(token.value != SQLTokenizer.WHEN){
previousToken();
input = expression(cmd, 0);
token = nextToken(MISSING_WHEN_ELSE_END);
}
while(true){
switch(token.value){
case SQLTokenizer.WHEN:
Expression condition = expression(cmd, 0);
if(input != null){
condition = new ExpressionArithmetic( input, condition, ExpressionArithmetic.EQUALS);
}
nextToken(MISSING_THEN);
Expression result = expression(cmd, 0);
expr.addCase(condition, result);
break;
case SQLTokenizer.ELSE:
expr.setElseResult(expression(cmd, 0));
break;
case SQLTokenizer.END:
expr.setEnd();
return expr;
default:
throw new Error();
}
token = nextToken(MISSING_WHEN_ELSE_END);
}
}
private Expression function( Command cmd, SQLToken token, boolean isEscape ) throws SQLException{
Expression expr;
switch(token.value){
case SQLTokenizer.CONVERT:{
Column col;
Expression style = null;
if(isEscape){
expr = expression( cmd, 0);
nextToken(MISSING_COMMA);
col = datatype(isEscape);
}else{
col = datatype(isEscape);
nextToken(MISSING_COMMA);
expr = expression( cmd, 0);
token = nextToken(MISSING_COMMA_PARENTHESIS);
if(token.value == SQLTokenizer.COMMA){
style = expression( cmd, 0);
}else
previousToken();
}
nextToken(MISSING_PARENTHESIS_R);
return new ExpressionFunctionConvert( col, expr, style );
}
case SQLTokenizer.CAST:
expr = expression( cmd, 0);
nextToken(MISSING_AS);
Column col = datatype(false);
nextToken(MISSING_PARENTHESIS_R);
return new ExpressionFunctionConvert( col, expr, null );
case SQLTokenizer.TIMESTAMPDIFF:
token = nextToken(MISSING_INTERVALS);
nextToken(MISSING_COMMA);
expr = expression( cmd, 0);
nextToken(MISSING_COMMA);
expr = new ExpressionFunctionTimestampDiff( token.value, expr, expression( cmd, 0));
nextToken(MISSING_PARENTHESIS_R);
return expr;
case SQLTokenizer.TIMESTAMPADD:
token = nextToken(MISSING_INTERVALS);
nextToken(MISSING_COMMA);
expr = expression( cmd, 0);
nextToken(MISSING_COMMA);
expr = new ExpressionFunctionTimestampAdd( token.value, expr, expression( cmd, 0));
nextToken(MISSING_PARENTHESIS_R);
return expr;
}
Expressions paramList = expressionParenthesisList(cmd);
int paramCount = paramList.size();
Expression[] params = paramList.toArray();
boolean invalidParamCount;
switch(token.value){
case SQLTokenizer.ABS:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionAbs();
break;
case SQLTokenizer.ACOS:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionACos();
break;
case SQLTokenizer.ASIN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionASin();
break;
case SQLTokenizer.ATAN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionATan();
break;
case SQLTokenizer.ATAN2:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionATan2();
break;
case SQLTokenizer.CEILING:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionCeiling();
break;
case SQLTokenizer.COS:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionCos();
break;
case SQLTokenizer.COT:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionCot();
break;
case SQLTokenizer.DEGREES:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionDegrees();
break;
case SQLTokenizer.EXP:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionExp();
break;
case SQLTokenizer.FLOOR:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionFloor();
break;
case SQLTokenizer.LOG:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionLog();
break;
case SQLTokenizer.LOG10:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionLog10();
break;
case SQLTokenizer.MOD:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionMod();
break;
case SQLTokenizer.PI:
invalidParamCount = (paramCount != 0);
expr = new ExpressionFunctionPI();
break;
case SQLTokenizer.POWER:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionPower();
break;
case SQLTokenizer.RADIANS:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionRadians();
break;
case SQLTokenizer.RAND:
invalidParamCount =  (paramCount != 0) && (paramCount != 1);
expr = new ExpressionFunctionRand();
break;
case SQLTokenizer.ROUND:
invalidParamCount =  (paramCount != 2);
expr = new ExpressionFunctionRound();
break;
case SQLTokenizer.SIN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionSin();
break;
case SQLTokenizer.SIGN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionSign();
break;
case SQLTokenizer.SQRT:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionSqrt();
break;
case SQLTokenizer.TAN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionTan();
break;
case SQLTokenizer.TRUNCATE:
invalidParamCount =  (paramCount != 2);
expr = new ExpressionFunctionTruncate();
break;
case SQLTokenizer.ASCII:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionAscii();
break;
case SQLTokenizer.BITLEN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionBitLen();
break;
case SQLTokenizer.CHARLEN:
case SQLTokenizer.CHARACTLEN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionCharLen();
break;
case SQLTokenizer.CHAR:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionChar();
break;
case SQLTokenizer.CONCAT:
if(paramCount != 2){
invalidParamCount = true;
expr = null;
break;
}
invalidParamCount = false;
expr = new ExpressionArithmetic( params[0], params[1], ExpressionArithmetic.ADD);
break;
case SQLTokenizer.DIFFERENCE:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionDifference();
break;
case SQLTokenizer.INSERT:
invalidParamCount = (paramCount != 4);
expr = new ExpressionFunctionInsert();
break;
case SQLTokenizer.LCASE:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionLCase();
break;
case SQLTokenizer.LEFT:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionLeft();
break;
case SQLTokenizer.LENGTH:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionLength();
break;
case SQLTokenizer.LOCATE:
invalidParamCount = (paramCount != 2) && (paramCount != 3);
expr = new ExpressionFunctionLocate();
break;
case SQLTokenizer.LTRIM:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionLTrim();
break;
case SQLTokenizer.OCTETLEN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionOctetLen();
break;
case SQLTokenizer.REPEAT:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionRepeat();
break;
case SQLTokenizer.REPLACE:
invalidParamCount = (paramCount != 3);
expr = new ExpressionFunctionReplace();
break;
case SQLTokenizer.RIGHT:
invalidParamCount = (paramCount != 2);
expr = new ExpressionFunctionRight();
break;
case SQLTokenizer.RTRIM:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionRTrim();
break;
case SQLTokenizer.SPACE:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionSpace();
break;
case SQLTokenizer.SOUNDEX:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionSoundex();
break;
case SQLTokenizer.SUBSTRING:
invalidParamCount = (paramCount != 3);
expr = new ExpressionFunctionSubstring();
break;
case SQLTokenizer.UCASE:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionUCase();
break;
case SQLTokenizer.CURDATE:
case SQLTokenizer.CURRENTDATE:
invalidParamCount = (paramCount != 0);
expr = new ExpressionValue( new DateTime(DateTime.now(), SQLTokenizer.DATE), SQLTokenizer.DATE);
break;
case SQLTokenizer.CURTIME:
invalidParamCount = (paramCount != 0);
expr = new ExpressionValue( new DateTime(DateTime.now(), SQLTokenizer.TIME), SQLTokenizer.TIME);
break;
case SQLTokenizer.DAYOFMONTH:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionDayOfMonth();
break;
case SQLTokenizer.DAYOFWEEK:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionDayOfWeek();
break;
case SQLTokenizer.DAYOFYEAR:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionDayOfYear();
break;
case SQLTokenizer.HOUR:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionHour();
break;
case SQLTokenizer.MINUTE:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionMinute();
break;
case SQLTokenizer.MONTH:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionMonth();
break;
case SQLTokenizer.NOW:
invalidParamCount = (paramCount != 0);
expr = new ExpressionValue( new DateTime(DateTime.now(), SQLTokenizer.TIMESTAMP), SQLTokenizer.TIMESTAMP);
break;
case SQLTokenizer.YEAR:
invalidParamCount = (paramCount != 1);
expr = new ExpressionFunctionYear();
break;
case SQLTokenizer.IIF:
invalidParamCount = (paramCount != 3);
expr = new ExpressionFunctionIIF();
break;
case SQLTokenizer.SWITCH:
invalidParamCount = (paramCount % 2 != 0);
ExpressionFunctionCase exprCase = new ExpressionFunctionCase();
for(int i=0; i < paramCount-1; i +=2)
exprCase.addCase(params[i], params[i+1] );
exprCase.setEnd();
expr = exprCase;
break;
case SQLTokenizer.IFNULL:
switch(paramCount){
case 1:
return new ExpressionArithmetic( params[0], ExpressionArithmetic.ISNULL );
case 2:
invalidParamCount = false;
expr = new ExpressionFunctionIIF();
Expression[] newParams = new Expression[3];
newParams[0] = new ExpressionArithmetic( params[0], ExpressionArithmetic.ISNULL );
newParams[1] = params[1];
newParams[2] = params[0];
params = newParams;
paramCount = 3;
break;
default:
invalidParamCount = true;
expr = null; 
}
break;
case SQLTokenizer.COUNT:
invalidParamCount = (paramCount != 1);
if(params[0].getType() == Expression.NAME){
ExpressionName param = (ExpressionName)params[0];
if("*".equals(param.getName()) && param.getTableAlias() == null){
params[0] = new ExpressionValue("*", SQLTokenizer.VARCHAR);
}
}
expr = new ExpressionName( Expression.COUNT );
break;
case SQLTokenizer.SUM:
invalidParamCount = (paramCount != 1);
expr = new ExpressionName( Expression.SUM );
break;
case SQLTokenizer.MAX:
invalidParamCount = (paramCount != 1);
expr = new ExpressionName( Expression.MAX );
break;
case SQLTokenizer.MIN:
invalidParamCount = (paramCount != 1);
expr = new ExpressionName( Expression.MIN );
break;
case SQLTokenizer.FIRST:
invalidParamCount = (paramCount != 1);
expr = new ExpressionName( Expression.FIRST );
break;
case SQLTokenizer.LAST:
invalidParamCount = (paramCount != 1);
expr = new ExpressionName( Expression.LAST );
break;
case SQLTokenizer.AVG:
if(paramCount != 1){
invalidParamCount = true;
expr = null;
break;
}
expr = new ExpressionName( Expression.SUM );
expr.setParams( params );
Expression expr2 = new ExpressionName( Expression.COUNT );
expr2.setParams( params );
expr = new ExpressionArithmetic( expr, expr2, ExpressionArithmetic.DIV );
return expr;
default:
throw createSyntaxError(token, Language.STXADD_FUNC_UNKNOWN);
}
if(invalidParamCount) {
throw createSyntaxError(token, Language.STXADD_PARAM_INVALID_COUNT);
}
expr.setParams( params );
return expr;
}
private RowSource tableSource( Command cmd, DataSources tables) throws SQLException{
SQLToken token = nextToken(MISSING_EXPRESSION);
switch(token.value){
case SQLTokenizer.PARENTHESIS_L: 
return rowSource( cmd, tables, SQLTokenizer.PARENTHESIS_R );
case SQLTokenizer.ESCAPE_L: 
token = nextToken(MISSING_OJ);
return rowSource( cmd, tables, SQLTokenizer.ESCAPE_R );
case SQLTokenizer.SELECT:
ViewResult viewResult = new ViewResult( con, select() );
tables.add(viewResult);
return viewResult;
}
String catalog = null;
String name = getIdentifier( token );
token = nextToken();
if(token != null && token.value == SQLTokenizer.POINT){
catalog = name;
name = nextIdentifier();
token = nextToken();
}
TableView tableView = Database.getTableView( con, catalog, name);
TableViewResult table = TableViewResult.createResult(tableView);
tables.add( table );
if(token != null && token.value == SQLTokenizer.AS){
token = nextToken(MISSING_EXPRESSION);
table.setAlias( token.getName( sql ) );
}else{
previousToken();
}
return table;
}
private Join join(Command cmd, DataSources tables, RowSource left, int type) throws SQLException{
RowSource right = rowSource(cmd, tables, 0);
SQLToken token = nextToken();
while(true){
if(token == null) {
throw createSyntaxError(token, Language.STXADD_JOIN_INVALID);
}
switch(token.value){
case SQLTokenizer.ON:
if(type == Join.RIGHT_JOIN)
return new Join( Join.LEFT_JOIN, right, left, expression( cmd, 0 ) );
return new Join( type, left, right, expression( cmd, 0 ) );
default:
if(!right.hasAlias()){
right.setAlias( token.getName( sql ) );
token = nextToken();
continue;
}
throw createSyntaxError( token, MISSING_ON );
}
}
}
private RowSource rowSource(Command cmd, DataSources tables, int parenthesis) throws SQLException{
RowSource fromSource = null;
fromSource = tableSource(cmd, tables);
while(true){
SQLToken token = nextToken();
if(token == null) return fromSource;
switch(token.value){
case SQLTokenizer.ON:
previousToken();
return fromSource;
case SQLTokenizer.CROSS:
nextToken(MISSING_JOIN);
case SQLTokenizer.COMMA:
fromSource = new Join( Join.CROSS_JOIN, fromSource, rowSource(cmd, tables, 0), null);
break;
case SQLTokenizer.INNER:
nextToken(MISSING_JOIN);
case SQLTokenizer.JOIN:
fromSource = join( cmd, tables, fromSource, Join.INNER_JOIN );
break;
case SQLTokenizer.LEFT:
token = nextToken(MISSING_OUTER_JOIN);
if(token.value == SQLTokenizer.OUTER)
token = nextToken(MISSING_JOIN);
fromSource = join( cmd, tables, fromSource, Join.LEFT_JOIN );
break;
case SQLTokenizer.RIGHT:
token = nextToken(MISSING_OUTER_JOIN);
if(token.value == SQLTokenizer.OUTER)
token = nextToken(MISSING_JOIN);
fromSource = join( cmd, tables, fromSource, Join.RIGHT_JOIN );
break;
case SQLTokenizer.FULL:
token = nextToken(MISSING_OUTER_JOIN);
if(token.value == SQLTokenizer.OUTER)
token = nextToken(MISSING_JOIN);
fromSource = join( cmd, tables, fromSource, Join.FULL_JOIN );
break;
case SQLTokenizer.PARENTHESIS_R:
case SQLTokenizer.ESCAPE_R:
if(parenthesis == token.value) return fromSource;
if(parenthesis == 0){
previousToken();
return fromSource;
}
throw createSyntaxError( token, Language.STXADD_FROM_PAR_CLOSE );
default:
if(isKeyword(token)){
previousToken();
return fromSource;
}
if(!fromSource.hasAlias()){
fromSource.setAlias( token.getName( sql ) );
break;
}
throw createSyntaxError( token, new int[]{SQLTokenizer.COMMA, SQLTokenizer.GROUP, SQLTokenizer.ORDER, SQLTokenizer.HAVING} );
}
}
}
private void from(CommandSelect cmd) throws SQLException{
DataSources tables = new DataSources();
cmd.setTables(tables);
cmd.setSource( rowSource( cmd, tables, 0 ) );
SQLToken token;
while(null != (token = nextToken())){
switch(token.value){
case SQLTokenizer.WHERE:
where( cmd );
break;
case SQLTokenizer.GROUP:
group( cmd );
break;
case SQLTokenizer.HAVING:
having( cmd );
break;
default:
previousToken();
return;
}
}
}
private void order(CommandSelect cmd) throws SQLException{
nextToken(MISSING_BY);
cmd.setOrder(expressionTokenList(cmd, SQLTokenizer.ORDER));
}
private void limit(CommandSelect selCmd) throws SQLException{
SQLToken token = nextToken(MISSING_EXPRESSION);
try{
int maxRows = Integer.parseInt(token.getName(sql));
selCmd.setMaxRows(maxRows);
}catch(NumberFormatException e){
throw createSyntaxError(token, Language.STXADD_NOT_NUMBER, token.getName(sql));
}
}
private void group(CommandSelect cmd) throws SQLException{
nextToken(MISSING_BY);
cmd.setGroup( expressionTokenList(cmd, SQLTokenizer.GROUP) );
}
private void where(CommandSelect cmd) throws SQLException{
cmd.setWhere( expression(cmd, 0) );
}
private void having(CommandSelect cmd) throws SQLException{
cmd.setHaving( expression(cmd, 0) );
}
private static final int[] COMMANDS = {SQLTokenizer.SELECT, SQLTokenizer.DELETE, SQLTokenizer.INSERT, SQLTokenizer.UPDATE, SQLTokenizer.CREATE, SQLTokenizer.DROP, SQLTokenizer.ALTER, SQLTokenizer.SET, SQLTokenizer.USE, SQLTokenizer.EXECUTE, SQLTokenizer.TRUNCATE};
private static final int[] COMMANDS_ESCAPE = {SQLTokenizer.D, SQLTokenizer.T, SQLTokenizer.TS, SQLTokenizer.FN, SQLTokenizer.CALL};
private static final int[] COMMANDS_ALTER = {SQLTokenizer.DATABASE, SQLTokenizer.TABLE, SQLTokenizer.VIEW,  SQLTokenizer.PROCEDURE, };
private static final int[] COMMANDS_CREATE = {SQLTokenizer.DATABASE, SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.INDEX, SQLTokenizer.PROCEDURE, SQLTokenizer.UNIQUE, SQLTokenizer.CLUSTERED, SQLTokenizer.NONCLUSTERED};
private static final int[] COMMANDS_DROP = {SQLTokenizer.DATABASE, SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.INDEX, SQLTokenizer.PROCEDURE};
private static final int[] COMMANDS_SET = {SQLTokenizer.TRANSACTION};
private static final int[] COMMANDS_CREATE_UNIQUE = {SQLTokenizer.INDEX, SQLTokenizer.CLUSTERED, SQLTokenizer.NONCLUSTERED};
private static final int[] MISSING_TABLE = {SQLTokenizer.TABLE};
private static final int[] ESCAPE_MISSING_CLOSE = {SQLTokenizer.ESCAPE_R};
private static final int[] MISSING_EXPRESSION = {SQLTokenizer.VALUE};
private static final int[] MISSING_IDENTIFIER = {SQLTokenizer.IDENTIFIER};
private static final int[] MISSING_BY = {SQLTokenizer.BY};
private static final int[] MISSING_PARENTHESIS_L = {SQLTokenizer.PARENTHESIS_L};
private static final int[] MISSING_PARENTHESIS_R = {SQLTokenizer.PARENTHESIS_R};
private static final int[] MISSING_DATATYPE  = {SQLTokenizer.BIT, SQLTokenizer.BOOLEAN, SQLTokenizer.BINARY, SQLTokenizer.VARBINARY, SQLTokenizer.RAW, SQLTokenizer.LONGVARBINARY, SQLTokenizer.BLOB, SQLTokenizer.TINYINT, SQLTokenizer.SMALLINT, SQLTokenizer.INT, SQLTokenizer.COUNTER, SQLTokenizer. BIGINT, SQLTokenizer.SMALLMONEY, SQLTokenizer.MONEY, SQLTokenizer.DECIMAL, SQLTokenizer.NUMERIC, SQLTokenizer.REAL, SQLTokenizer.FLOAT, SQLTokenizer.DOUBLE, SQLTokenizer.DATE, SQLTokenizer.TIME, SQLTokenizer.TIMESTAMP, SQLTokenizer.SMALLDATETIME, SQLTokenizer.CHAR, SQLTokenizer.NCHAR, SQLTokenizer.VARCHAR, SQLTokenizer.NVARCHAR, SQLTokenizer.LONG, SQLTokenizer.LONGNVARCHAR, SQLTokenizer.LONGVARCHAR, SQLTokenizer.CLOB, SQLTokenizer.NCLOB, SQLTokenizer.UNIQUEIDENTIFIER, SQLTokenizer.JAVA_OBJECT, SQLTokenizer.SYSNAME};
private static final int[] MISSING_SQL_DATATYPE = { SQLTokenizer.SQL_BIGINT , SQLTokenizer.SQL_BINARY , SQLTokenizer.SQL_BIT , SQLTokenizer.SQL_CHAR , SQLTokenizer.SQL_DATE , SQLTokenizer.SQL_DECIMAL , SQLTokenizer.SQL_DOUBLE , SQLTokenizer.SQL_FLOAT , SQLTokenizer.SQL_INTEGER , SQLTokenizer.SQL_LONGVARBINARY , SQLTokenizer.SQL_LONGVARCHAR , SQLTokenizer.SQL_REAL , SQLTokenizer.SQL_SMALLINT , SQLTokenizer.SQL_TIME , SQLTokenizer.SQL_TIMESTAMP , SQLTokenizer.SQL_TINYINT , SQLTokenizer.SQL_VARBINARY , SQLTokenizer.SQL_VARCHAR };
private static final int[] MISSING_INTO = {SQLTokenizer.INTO};
private static final int[] MISSING_BETWEEN_IN = {SQLTokenizer.BETWEEN, SQLTokenizer.IN};
private static final int[] MISSING_NOT_NULL = {SQLTokenizer.NOT, SQLTokenizer.NULL};
private static final int[] MISSING_NULL = {SQLTokenizer.NULL};
private static final int[] MISSING_COMMA = {SQLTokenizer.COMMA};
private static final int[] MISSING_COMMA_PARENTHESIS = {SQLTokenizer.COMMA, SQLTokenizer.PARENTHESIS_R};
private static final int[] MISSING_PARENTHESIS_VALUES_SELECT = {SQLTokenizer.PARENTHESIS_L, SQLTokenizer.VALUES, SQLTokenizer.SELECT};
private static final int[] MISSING_TOKEN_LIST = {SQLTokenizer.COMMA, SQLTokenizer.FROM, SQLTokenizer.GROUP, SQLTokenizer.HAVING, SQLTokenizer.ORDER};
private static final int[] MISSING_FROM = {SQLTokenizer.FROM};
private static final int[] MISSING_SET = {SQLTokenizer.SET};
private static final int[] MISSING_EQUALS = {SQLTokenizer.EQUALS};
private static final int[] MISSING_WHERE = {SQLTokenizer.WHERE};
private static final int[] MISSING_WHERE_COMMA = {SQLTokenizer.WHERE, SQLTokenizer.COMMA};
private static final int[] MISSING_ISOLATION = {SQLTokenizer.ISOLATION};
private static final int[] MISSING_LEVEL = {SQLTokenizer.LEVEL};
private static final int[] COMMANDS_TRANS_LEVEL = {SQLTokenizer.READ, SQLTokenizer.REPEATABLE, SQLTokenizer.SERIALIZABLE};
private static final int[] MISSING_READ = {SQLTokenizer.READ};
private static final int[] MISSING_COMM_UNCOMM = {SQLTokenizer.COMMITTED, SQLTokenizer.UNCOMMITTED};
private static final int[] MISSING_OPTIONS_DATATYPE = { SQLTokenizer.DEFAULT, SQLTokenizer.IDENTITY, SQLTokenizer.NOT, SQLTokenizer.NULL, SQLTokenizer.PRIMARY, SQLTokenizer.UNIQUE, SQLTokenizer.COMMA, SQLTokenizer.PARENTHESIS_R};
private static final int[] MISSING_NUMBERVALUE = {SQLTokenizer.NUMBERVALUE};
private static final int[] MISSING_AND = {SQLTokenizer.AND};
private static final int[] MISSING_JOIN = {SQLTokenizer.JOIN};
private static final int[] MISSING_OUTER_JOIN = {SQLTokenizer.OUTER, SQLTokenizer.JOIN};
private static final int[] MISSING_OJ = {SQLTokenizer.OJ};
private static final int[] MISSING_ON = {SQLTokenizer.ON};
private static final int[] MISSING_KEYTYPE = {SQLTokenizer.PRIMARY, SQLTokenizer.UNIQUE, SQLTokenizer.FOREIGN};
private static final int[] MISSING_KEY = {SQLTokenizer.KEY};
private static final int[] MISSING_REFERENCES = {SQLTokenizer.REFERENCES};
private static final int[] MISSING_AS = {SQLTokenizer.AS};
private static final int[] MISSING_SELECT = {SQLTokenizer.SELECT};
private static final int[] MISSING_INTERVALS = {SQLTokenizer.SQL_TSI_FRAC_SECOND, SQLTokenizer.SQL_TSI_SECOND, SQLTokenizer.SQL_TSI_MINUTE, SQLTokenizer.SQL_TSI_HOUR, SQLTokenizer.SQL_TSI_DAY, SQLTokenizer.SQL_TSI_WEEK, SQLTokenizer.SQL_TSI_MONTH, SQLTokenizer.SQL_TSI_QUARTER, SQLTokenizer.SQL_TSI_YEAR, SQLTokenizer.MILLISECOND, SQLTokenizer.SECOND, SQLTokenizer.MINUTE, SQLTokenizer.HOUR, SQLTokenizer.DAY, SQLTokenizer.WEEK, SQLTokenizer.MONTH, SQLTokenizer.QUARTER, SQLTokenizer.YEAR, SQLTokenizer.D};
private static final int[] MISSING_ALL = {SQLTokenizer.ALL};
private static final int[] MISSING_THEN = {SQLTokenizer.THEN};
private static final int[] MISSING_WHEN_ELSE_END = {SQLTokenizer.WHEN, SQLTokenizer.ELSE, SQLTokenizer.END};
private static final int[] MISSING_ADD_ALTER_DROP = {SQLTokenizer.ADD, SQLTokenizer.ALTER, SQLTokenizer.DROP};
}
package smallsql.database;
import smallsql.database.language.Language;
class ExpressionInSelect extends ExpressionArithmetic {
final private CommandSelect cmdSel;
final private Index index = new Index(true);
final private SSConnection con;
ExpressionInSelect(SSConnection con, Expression left, CommandSelect cmdSel, int operation) {
super(left, (Expressions)null, operation);
this.cmdSel = cmdSel;
this.con = con;
}
private void loadInList() throws Exception{
if(cmdSel.compile(con)){
cmdSel.from.execute();
if(cmdSel.columnExpressions.size() != 1)
throw SmallSQLException.create(Language.SUBQUERY_COL_COUNT, new Integer(cmdSel.columnExpressions.size()));
index.clear();
while(cmdSel.next()){
try{
index.addValues(0, cmdSel.columnExpressions );
}catch(Exception e){
}
}
}
}
boolean isInList() throws Exception{
loadInList();
return index.findRows(getParams(), false, null) != null;
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
final class LongTreeList {
private byte[] data;
private int size;
private int offset;
static final private int pointerSize = 3; 
LongTreeList(){
data = new byte[25];
}
LongTreeList(long value) throws SQLException{
this();
add(value);
}
LongTreeList(StoreImpl input){
int readSize = input.readInt();
data     = input.readBytes(readSize);
}
final void save(StoreImpl output){
output.writeInt(size);
output.writeBytes(data, 0, size);
}
final void add(long value) throws SQLException{
offset = 0;
if(size == 0){
writeShort( (int)(value >> 48) );
writePointer ( offset+pointerSize+2 );
writeShort( 0 );
writeShort( (int)(value >> 32) );
writePointer ( offset+pointerSize+2 );
writeShort( 0 );
writeShort( (int)(value >> 16) );
writePointer ( offset+pointerSize+2 );
writeShort( 0 );
writeShort( (int)(value) );
writeShort( 0 );
size = offset;
return;
}
int shift = 48;
boolean firstNode = (size > 2); 
while(shift>=0){
int octet = (int)(value >> shift) & 0xFFFF;
while(true){
int nextEntry = getUnsignedShort();
if(nextEntry == octet){
if(shift == 0) return; 
offset = getPointer();
firstNode = true;
break;
}
if((nextEntry == 0 && !firstNode) || nextEntry > octet){
offset -= 2;
while(true){
if(shift != 0){
offset = insertNode(octet);
}else{
insertNodeLastLevel(octet);
return;
}
shift -= 16;
octet = (int)(value >> shift) & 0xFFFF;
}
}
firstNode = false;
if(shift != 0) offset += pointerSize;
}
shift -= 16;
}
}
final void remove(long value) throws SQLException{
if(size == 0) return;
int offset1 = 0;
int offset2 = 0;
int offset3 = 0;
offset = 0;
int shift = 48;
boolean firstNode = true; 
boolean firstNode1 = true;
boolean firstNode2 = true;
boolean firstNode3 = true;
while(shift>=0){
int octet = (int)(value >> shift) & 0xFFFF;
while(true){
int nextEntry = getUnsignedShort();
if(nextEntry == octet){
if(shift == 0){
offset -= 2;
removeNodeLastLevel();
while(firstNode && getUnsignedShort() == 0){
offset -= 2;
removeNodeLastLevel(); 
if(shift >= 3)
break;
offset = offset1;
offset1 = offset2;
offset2 = offset3;
firstNode = firstNode1;
firstNode1 = firstNode2;
firstNode2 = firstNode3;
removeNode();
shift++;
}
return;
}
offset3 = offset2;
offset2 = offset1;
offset1 = offset -2;
offset = getPointer();
firstNode3 = firstNode2;
firstNode2 = firstNode1;
firstNode1 = firstNode;
firstNode = true;
break;
}
if((nextEntry == 0 && !firstNode) || nextEntry > octet){
return;
}
firstNode = false;
if(shift != 0) offset += pointerSize;
}
shift -= 16;
}
}
final long getNext(LongTreeListEnum listEnum){
int shift = (3-listEnum.stack) << 4;
if(shift >= 64) return -1; 
offset 		= listEnum.offsetStack[listEnum.stack];
long result = listEnum.resultStack[listEnum.stack];
boolean firstNode = (offset == 0); 
while(true){
int nextEntry = getUnsignedShort();
if(nextEntry != 0 || firstNode){
result |= (((long)nextEntry) << shift);
if(listEnum.stack>=3){
listEnum.offsetStack[listEnum.stack] = offset;
return result;
}
listEnum.offsetStack[listEnum.stack] = offset+pointerSize;
offset = getPointer();
shift -= 16;
listEnum.stack++;
listEnum.resultStack[listEnum.stack] = result;
firstNode = true;
}else{
shift += 16;
listEnum.stack--;
if(listEnum.stack<0) return -1; 
result = listEnum.resultStack[listEnum.stack];
offset = listEnum.offsetStack[listEnum.stack];
firstNode = false;
}
}
}
final long getPrevious(LongTreeListEnum listEnum){
int shift = (3-listEnum.stack) << 4;
if(shift >= 64){ 
shift = 48;
offset = 0;
listEnum.stack = 0;
listEnum.offsetStack[0] = 2 + pointerSize;
loopToEndOfNode(listEnum);
}else{
setPreviousOffset(listEnum);
}
long result = listEnum.resultStack[listEnum.stack];
while(true){
int nextEntry = (offset < 0) ? -1 : getUnsignedShort();
if(nextEntry >= 0){
result |= (((long)nextEntry) << shift);
if(listEnum.stack>=3){
listEnum.offsetStack[listEnum.stack] = offset;
return result;
}
listEnum.offsetStack[listEnum.stack] = offset+pointerSize;
offset = getPointer();
shift -= 16;
listEnum.stack++;
listEnum.resultStack[listEnum.stack] = result;
loopToEndOfNode(listEnum);
}else{
shift += 16;
listEnum.stack--;
if(listEnum.stack<0) return -1; 
result = listEnum.resultStack[listEnum.stack];
setPreviousOffset(listEnum);
}
}
}
final private void setPreviousOffset(LongTreeListEnum listEnum){
int previousOffset = listEnum.offsetStack[listEnum.stack] - 2*(2 + (listEnum.stack>=3 ? 0 : pointerSize));
if(listEnum.stack == 0){
offset = previousOffset;
return;
}
offset = listEnum.offsetStack[listEnum.stack-1] - pointerSize;
int pointer = getPointer();
if(pointer <= previousOffset){
offset = previousOffset;
return;
}
offset = -1;
}
final private void loopToEndOfNode(LongTreeListEnum listEnum){
int nextEntry;
int nextOffset1, nextOffset2;
nextOffset1 = offset;
offset += 2;
if(listEnum.stack<3)
offset += pointerSize;
do{
nextOffset2 = nextOffset1;
nextOffset1 = offset;
nextEntry = getUnsignedShort();
if(listEnum.stack<3)
offset += pointerSize;
}while(nextEntry != 0);
offset = nextOffset2;
}
final private int insertNode(int octet) throws SQLException{
int oldOffset = offset;
if(data.length < size + 4 + pointerSize) resize();
System.arraycopy(data, oldOffset, data, oldOffset + 2+pointerSize, size-oldOffset);
size += 2+pointerSize;
writeShort( octet );
writePointer( size );
correctPointers( 0, oldOffset, 2+pointerSize, 0 );
data[size++] = (byte)0;
data[size++] = (byte)0;
return size-2;
}
final private void insertNodeLastLevel(int octet) throws SQLException{
int oldOffset = offset;
if(data.length < size + 2) resize();
System.arraycopy(data, offset, data, offset + 2, size-offset);
size += 2;
writeShort( octet );
correctPointers( 0, oldOffset, 2, 0 );
}
final private void removeNode() throws SQLException{
int oldOffset = offset;
correctPointers( 0, oldOffset, -(2+pointerSize), 0 );
size -= 2+pointerSize;
System.arraycopy(data, oldOffset + 2+pointerSize, data, oldOffset, size-oldOffset);
offset = oldOffset;
}
final private void removeNodeLastLevel() throws SQLException{
int oldOffset = offset;
correctPointers( 0, oldOffset, -2, 0 );
size -= 2;
System.arraycopy(data, oldOffset + 2, data, oldOffset, size-oldOffset);
offset = oldOffset;
}
final private void correctPointers(int startOffset, int oldOffset, int diff, int level){
offset = startOffset;
boolean firstNode = true;
while(offset < size){
if(offset == oldOffset){
int absDiff = Math.abs(diff);
if(absDiff == 2) return;
offset += absDiff;
firstNode = false;
continue;
}
int value = getUnsignedShort();
if(value != 0 || firstNode){
int pointer = getPointer();
if(pointer > oldOffset){
offset  -= pointerSize;
writePointer( pointer + diff );
if(diff > 0) pointer += diff;
}
if(level < 2){
startOffset = offset;
correctPointers( pointer, oldOffset, diff, level+1);
offset = startOffset;
}
firstNode = false;
}else{
return;
}
}
}
final private int getPointer(){
int value = 0;
for(int i=0; i<pointerSize; i++){
value <<= 8;
value += (data[offset++] & 0xFF);
}
return value;
}
final private void writePointer(int value){
for(int i=pointerSize-1; i>=0; i--){
data[offset++] = (byte)(value >> (i*8));
}
}
final private int getUnsignedShort(){
return ((data[ offset++ ] & 0xFF) << 8) | (data[ offset++ ] & 0xFF);
}
final private void writeShort(int value){
data[offset++] = (byte)(value >> 8);
data[offset++] = (byte)(value);
}
private final void resize() throws SQLException{
int newsize = data.length << 1;
if(newsize > 0xFFFFFF){ 
newsize = 0xFFFFFF;
if(newsize == data.length) throw SmallSQLException.create(Language.INDEX_TOOMANY_EQUALS);
}
byte[] temp = new byte[newsize];
System.arraycopy(data, 0, temp, 0, data.length);
data = temp;
}
final int getSize() {
return size;
}
}
package smallsql.database;
import java.io.File;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import smallsql.database.language.Language;
public class CreateFile extends TransactionStep{
private final File file;
private final SSConnection con;
private final Database database;
CreateFile(File file, FileChannel raFile,SSConnection con, Database database){
super(raFile);
this.file = file;
this.con = con;
this.database = database;
}
@Override
long commit(){
raFile = null;
return -1;
}
@Override
void rollback() throws SQLException{
FileChannel currentRaFile = raFile;
if(raFile == null){
return;
}
raFile = null;
try{
currentRaFile.close();
}catch(Throwable ex){
}
con.rollbackFile(currentRaFile);
if(!file.delete()){
file.deleteOnExit();
throw SmallSQLException.create(Language.FILE_CANT_DELETE, file.getPath());
}
String name = file.getName();
name = name.substring(0, name.lastIndexOf('.'));
database.removeTableView(name);
}
}
package smallsql.database;
public class ExpressionFunctionRight extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.RIGHT;
}
final boolean isNull() throws Exception {
return param1.isNull() || param2.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int length = param2.getInt();
if(bytes.length <= length) return bytes;
byte[] b = new byte[length];
System.arraycopy(bytes, bytes.length -length, b, 0, length);
return b;
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int length  = param2.getInt();
int start = str.length() - Math.min( length, str.length() );
return str.substring(start);
}
}
package smallsql.database;
public class ExpressionFunctionLCase extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.LCASE;
}
final boolean isNull() throws Exception {
return param1.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
return getString().getBytes();
}
final String getString() throws Exception {
if(isNull()) return null;
return param1.getString().toLowerCase();
}
}
package smallsql.database;
final class ExpressionFunctionBitLen extends ExpressionFunctionReturnInt {
private static final int BYTES_PER_CHAR = 2;
final int getFunction() {
return SQLTokenizer.BITLEN;
}
boolean isNull() throws Exception {
return param1.isNull();
}
final int getInt() throws Exception {
if(isNull()) return 0;
String str = param1.getString();
return str.length() * BYTES_PER_CHAR * 8;
}
}
package smallsql.database;
import java.sql.SQLException;
abstract class Expression implements Cloneable{
static final Expression NULL = new ExpressionValue( null, SQLTokenizer.NULL );
final private int type;
private String name; 
private String alias;
private Expression[] params;
Expression(int type){
this.type = type;
}
protected Object clone() throws CloneNotSupportedException{
return super.clone();
}
final String getName(){
return name;
}
final void setName(String name){
this.alias = this.name = name;
}
final String getAlias(){
return alias;
}
final void setAlias(String alias){
this.alias = alias;
}
void setParams( Expression[] params ){
this.params = params;
}
void setParamAt( Expression param, int idx){
params[idx] = param;
}
final Expression[] getParams(){ return params; }
void optimize() throws SQLException{
if(params != null){
for(int p=0; p<params.length; p++){
params[p].optimize();
}
}
}
public boolean equals(Object expr){
if(!(expr instanceof Expression)) return false;
if( ((Expression)expr).type == type){
Expression[] p1 = ((Expression)expr).params;
Expression[] p2 = params;
if(p1 != null && p2 != null){
if(p1 == null) return false;
for(int i=0; i<p1.length; i++){
if(!p2[i].equals(p1[i])) return false;
}
}
String name1 = ((Expression)expr).name;
String name2 = name;
if(name1 == name2) return true;
if(name1 == null) return false;
if(name1.equalsIgnoreCase(name2)) return true;
}
return false;
}
abstract boolean isNull() throws Exception;
abstract boolean getBoolean() throws Exception;
abstract int getInt() throws Exception;
abstract long getLong() throws Exception;
abstract float getFloat() throws Exception;
abstract double getDouble() throws Exception;
abstract long getMoney() throws Exception;
abstract MutableNumeric getNumeric() throws Exception;
abstract Object getObject() throws Exception;
final Object getApiObject() throws Exception{
Object obj = getObject();
if(obj instanceof Mutable){
return ((Mutable)obj).getImmutableObject();
}
return obj;
}
abstract String getString() throws Exception;
abstract byte[] getBytes() throws Exception;
abstract int getDataType();
final int getType(){return type;}
String getTableName(){
return null;
}
int getPrecision(){
return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
}
int getScale(){
return getScale(getDataType());
}
final static int getScale(int dataType){
switch(dataType){
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return 4;
case SQLTokenizer.TIMESTAMP:
return 9; 
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return 38;
default: return 0;
}
}
int getDisplaySize(){
return SSResultSetMetaData.getDisplaySize(getDataType(), getPrecision(), getScale());
}
boolean isDefinitelyWritable(){
return false;
}
boolean isAutoIncrement(){
return false;
}
boolean isCaseSensitive(){
return false;
}
boolean isNullable(){
return true;
}
static final int VALUE      = 1;
static final int NAME       = 2;
static final int FUNCTION   = 3;
static final int GROUP_BY   = 11;
static final int COUNT	    = 12;
static final int SUM	    = 13;
static final int FIRST		= 14;
static final int LAST		= 15;
static final int MIN		= 16;
static final int MAX		= 17;
static final int GROUP_BEGIN= GROUP_BY;
}
package smallsql.database;
public class CommandCreateView extends Command{
private Columns columns = new Columns();
String sql;
CommandCreateView( Logger log, String name ){
super(log);
this.type = SQLTokenizer.VIEW;
this.name = name;
}
void addColumn( Column column ){
columns.add( column );
}
void executeImpl(SSConnection con, SSStatement st) throws Exception{
con.getDatabase(false).createView(con, name, sql);
}
}
package smallsql.database;
final class MutableInteger extends Number implements Mutable{
int value;
MutableInteger(int value){
this.value = value;
}
public double doubleValue() {
return value;
}
public float floatValue() {
return value;
}
public int intValue() {
return value;
}
public long longValue() {
return value;
}
public String toString(){
return String.valueOf(value);
}
public Object getImmutableObject(){
return Utils.getInteger(value);
}
}
package smallsql.database;
public class CommandSet extends Command {
int isolationLevel;
CommandSet( Logger log, int type ){
super(log);
this.type = type;
}
void executeImpl(SSConnection con, SSStatement st) throws java.sql.SQLException {
switch(type){
case SQLTokenizer.LEVEL:
con.isolationLevel = isolationLevel;
break;
case SQLTokenizer.USE:
con.setCatalog(name);
break;
default:
throw new Error();
}
}
}
package smallsql.database;
import java.sql.*;
import java.util.List;
final class TableResult extends TableViewResult{
final private Table table;
private List insertStorePages;
private long firstOwnInsert;
private long maxFileOffset;
TableResult(Table table){
this.table = table;
}
@Override
final boolean init( SSConnection con ) throws Exception{
if(super.init(con)){
Columns columns = table.columns;
offsets     = new int[columns.size()];
dataTypes   = new int[columns.size()];
for(int i=0; i<columns.size(); i++){
dataTypes[i] = columns.get(i).getDataType();
}
return true;
}
return false;
}
@Override
final void execute() throws Exception{
insertStorePages = table.getInserts(con);
firstOwnInsert = 0x4000000000000000L | insertStorePages.size();
maxFileOffset = table.raFile.size();
beforeFirst();
}
@Override
final TableView getTableView(){
return table;
}
@Override
final void deleteRow() throws SQLException{
store.deleteRow(con);
store = new StoreNull(store.getNextPagePos());
}
@Override
final void updateRow(Expression[] updateValues) throws Exception{
Columns tableColumns = table.columns;
int count = tableColumns.size();
StoreImpl newStore = table.getStoreTemp(con);
synchronized(con.getMonitor()){
((StoreImpl)this.store).createWriteLock();
for(int i=0; i<count; i++){
Expression src = updateValues[i];
if(src != null){
newStore.writeExpression( src, tableColumns.get(i) );
}else{
copyValueInto( i, newStore );
}
}
((StoreImpl)this.store).updateFinsh(con, newStore);
}
}
@Override
final void insertRow(Expression[] updateValues) throws Exception{
Columns tableColumns = table.columns;
int count = tableColumns.size();
StoreImpl store = table.getStoreInsert(con);
for(int i=0; i<count; i++){
Column tableColumn = tableColumns.get(i);
Expression src = updateValues[i];
if(src == null) src = tableColumn.getDefaultValue(con);
store.writeExpression( src, tableColumn );
}
store.writeFinsh( con );
insertStorePages.add(store.getLink());
}
private Store store = Store.NOROW;
private long afterLastValidFilePos;
final private boolean moveToRow() throws Exception{
if(filePos >= 0x4000000000000000L){
store = ((StorePageLink)insertStorePages.get( (int)(filePos & 0x3FFFFFFFFFFFFFFFL) )).getStore( table, con, lock);
}else{
store = (filePos < maxFileOffset) ? table.getStore( con, filePos, lock ) : null;
if(store == null){
if(insertStorePages.size() > 0){
filePos = 0x4000000000000000L;
store = ((StorePageLink)insertStorePages.get( (int)(filePos & 0x3FFFFFFFFFFFFFFFL) )).getStore( table, con, lock);
}
}
}
if(store != null){
if(!store.isValidPage()){
return false;
}
store.scanObjectOffsets( offsets, dataTypes );
afterLastValidFilePos = store.getNextPagePos();
return true;
}else{
filePos = -1;
noRow();
return false;
}
}
final private boolean moveToValidRow() throws Exception{
while(filePos >= 0){
if(moveToRow())
return true;
setNextFilePos();
}
row = 0;
return false;
}
@Override
final void beforeFirst(){
filePos = 0;
store = Store.NOROW;
row = 0;
}
@Override
final boolean first() throws Exception{
filePos = table.getFirstPage();
row = 1;
return moveToValidRow();
}
final private void setNextFilePos(){
if(filePos < 0) return; 
if(store == Store.NOROW)
filePos = table.getFirstPage(); 
else
if(filePos >= 0x4000000000000000L){
filePos++;
if((filePos & 0x3FFFFFFFFFFFFFFFL) >= insertStorePages.size()){
filePos = -1;
noRow();
}
}else
filePos = store.getNextPagePos();
}
@Override
final boolean next() throws Exception{
if(filePos < 0) return false;
setNextFilePos();
row++;
return moveToValidRow();
}
@Override
final void afterLast(){
filePos = -1;
noRow();
}
@Override
final int getRow(){
return row;
}
@Override
final long getRowPosition(){
return filePos;
}
@Override
final void setRowPosition(long rowPosition) throws Exception{
filePos = rowPosition;
if(filePos < 0 || !moveToRow()){
store = new StoreNull(store.getNextPagePos());
}
}
@Override
final boolean rowInserted(){
return filePos >= firstOwnInsert;
}
@Override
final boolean rowDeleted(){
if(store instanceof StoreNull && store != Store.NULL){
return true;
}
if(store instanceof StoreImpl &&
((StoreImpl)store).isRollback()){
return true;
}
return false;
}
@Override
final void nullRow(){
row = 0;
store = Store.NULL;
}
@Override
final void noRow(){
row = 0;
store = Store.NOROW;
}
@Override
final boolean isNull( int colIdx ) throws Exception{
return store.isNull( offsets[colIdx] );
}
@Override
final boolean getBoolean( int colIdx ) throws Exception{
return store.getBoolean( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final int getInt( int colIdx ) throws Exception{
return store.getInt( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final long getLong( int colIdx ) throws Exception{
return store.getLong( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final float getFloat( int colIdx ) throws Exception{
return store.getFloat( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final double getDouble( int colIdx ) throws Exception{
return store.getDouble( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final long getMoney( int colIdx ) throws Exception{
return store.getMoney( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final MutableNumeric getNumeric( int colIdx ) throws Exception{
return store.getNumeric( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final Object getObject( int colIdx ) throws Exception{
return store.getObject( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final String getString( int colIdx ) throws Exception{
return store.getString( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final byte[] getBytes( int colIdx ) throws Exception{
return store.getBytes( offsets[colIdx], dataTypes[colIdx] );
}
@Override
final int getDataType( int colIdx ){
return dataTypes[colIdx];
}
final private void copyValueInto( int colIdx, StoreImpl dst){
int offset = offsets[colIdx++];
int length = (colIdx < offsets.length ? offsets[colIdx] : store.getUsedSize()) - offset;
dst.copyValueFrom( (StoreImpl)store, offset, length);
}
}
package smallsql.database;
class JoinScroll{
private final Expression condition; 
final int type;
final RowSource left; 
final RowSource right;
private boolean isBeforeFirst = true;
private boolean isOuterValid = true;
private boolean[] isFullNotValid;
private int fullRightRowCounter;
private int fullRowCount;
private int fullReturnCounter = -1;
JoinScroll( int type, RowSource left, RowSource right, Expression condition ){
this.type = type;
this.condition = condition;
this.left = left;
this.right = right;
if(type == Join.FULL_JOIN){
isFullNotValid = new boolean[10];
}
}
void beforeFirst() throws Exception{
left.beforeFirst();
right.beforeFirst();
isBeforeFirst = true;
fullRightRowCounter = 0;
fullRowCount        = 0;
fullReturnCounter   = -1;
}
boolean next() throws Exception{
boolean result;
if(fullReturnCounter >=0){
do{
if(fullReturnCounter >= fullRowCount){
return false;
}
right.next();
}while(isFullNotValid[fullReturnCounter++]);
return true;
}
do{
if(isBeforeFirst){
result = left.next();
if(result){
result = right.first();
if(!result){
switch(type){
case Join.LEFT_JOIN:
case Join.FULL_JOIN:
isOuterValid = false;
isBeforeFirst = false;
right.nullRow();
return true;
}
}else fullRightRowCounter++;
}else{
if(type == Join.FULL_JOIN){
while(right.next()){
fullRightRowCounter++;
}
fullRowCount = fullRightRowCounter;
}
}
}else{
result = right.next();
if(!result){
switch(type){
case Join.LEFT_JOIN:
case Join.FULL_JOIN:
if(isOuterValid){
isOuterValid = false;
right.nullRow();
return true;
}
fullRowCount = Math.max( fullRowCount, fullRightRowCounter);
fullRightRowCounter = 0;
}
isOuterValid = true;
result = left.next();
if(result){
result = right.first();
if(!result){
switch(type){
case Join.LEFT_JOIN:
case Join.FULL_JOIN:
isOuterValid = false;
right.nullRow();
return true;
}
}else fullRightRowCounter++;
}
}else fullRightRowCounter++;
}
isBeforeFirst = false;
}while(result && !getBoolean());
isOuterValid = false;
if(type == Join.FULL_JOIN){
if(fullRightRowCounter >= isFullNotValid.length){
boolean[] temp = new boolean[fullRightRowCounter << 1];
System.arraycopy( isFullNotValid, 0, temp, 0, fullRightRowCounter);
isFullNotValid = temp;
}
if(!result){
if(fullRowCount == 0){
return false;
}
if(fullReturnCounter<0) {
fullReturnCounter = 0;
right.first();
left.nullRow();
}
while(isFullNotValid[fullReturnCounter++]){
if(fullReturnCounter >= fullRowCount){
return false;
}
right.next();
}
return true;
}else
isFullNotValid[fullRightRowCounter-1] = result;
}
return result;
}
private boolean getBoolean() throws Exception{
return type == Join.CROSS_JOIN || condition.getBoolean();
}
}
package smallsql.tools;
import java.io.*;
import java.sql.*;
import java.util.Properties;
import javax.swing.JOptionPane;
import smallsql.database.*;
public class CommandLine {
public static void main(String[] args) throws Exception {
System.out.println("SmallSQL Database command line tool\n");
Connection con = new SSDriver().connect("jdbc:smallsql", new Properties());
Statement st = con.createStatement();
if(args.length>0){
con.setCatalog(args[0]);
}
System.out.println("\tVersion: "+con.getMetaData().getDatabaseProductVersion());
System.out.println("\tCurrent database: "+con.getCatalog());
System.out.println();
System.out.println("\tUse the USE command to change the database context.");
System.out.println("\tType 2 times ENTER to execute any SQL command.");
StringBuffer command = new StringBuffer();
BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
while(true){
try {
String line;
try{
line = input.readLine();
}catch(IOException ex){
ex.printStackTrace();
JOptionPane.showMessageDialog( null, "You need to start the command line of the \nSmallSQL Database with a console window:\n\n       java -jar smallsql.jar\n\n" + ex, "Fatal Error", JOptionPane.OK_OPTION);
return;
}
if(line == null){
return; 
}
if(line.length() == 0 && command.length() > 0){
boolean isRS = st.execute(command.toString());
if(isRS){
printRS(st.getResultSet());
}
command.setLength(0);
}
command.append(line).append('\n');
} catch (Exception e) {
command.setLength(0);
e.printStackTrace();
}
}
}
private static void printRS(ResultSet rs) throws SQLException {
ResultSetMetaData md = rs.getMetaData();
int count = md.getColumnCount();
for(int i=1; i<=count; i++){
System.out.print(md.getColumnLabel(i));
System.out.print('\t');
}
System.out.println();
while(rs.next()){
for(int i=1; i<=count; i++){
System.out.print(rs.getObject(i));
System.out.print('\t');
}
System.out.println();
}
}
}
package smallsql.database;
public class ExpressionFunctionLeft extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.LEFT;
}
final boolean isNull() throws Exception {
return param1.isNull() || param2.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int length = param2.getInt();
if(bytes.length <= length) return bytes;
byte[] b = new byte[length];
System.arraycopy(bytes, 0, b, 0, length);
return b;
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int length = param2.getInt();
length = Math.min( length, str.length() );
return str.substring(0,length);
}
}
package smallsql.database;
public class ExpressionFunctionTimestampDiff extends ExpressionFunction {
final private int interval;
static final int mapIntervalType(int intervalType){
switch(intervalType){
case SQLTokenizer.MILLISECOND:
return SQLTokenizer.SQL_TSI_FRAC_SECOND;
case SQLTokenizer.SECOND:
return SQLTokenizer.SQL_TSI_SECOND;
case SQLTokenizer.MINUTE:
return SQLTokenizer.SQL_TSI_MINUTE;
case SQLTokenizer.HOUR:
return SQLTokenizer.SQL_TSI_HOUR;
case SQLTokenizer.D:
case SQLTokenizer.DAY:
return SQLTokenizer.SQL_TSI_DAY;
case SQLTokenizer.WEEK:
return SQLTokenizer.SQL_TSI_WEEK;
case SQLTokenizer.MONTH:
return SQLTokenizer.SQL_TSI_MONTH;
case SQLTokenizer.QUARTER:
return SQLTokenizer.SQL_TSI_QUARTER;
case SQLTokenizer.YEAR:
return SQLTokenizer.SQL_TSI_YEAR;
default:
return intervalType;
}
}
ExpressionFunctionTimestampDiff(int intervalType, Expression p1, Expression p2){
interval = mapIntervalType( intervalType );
setParams( new Expression[]{p1,p2});
}
int getFunction() {
return SQLTokenizer.TIMESTAMPDIFF;
}
boolean isNull() throws Exception {
return param1.isNull() || param2.isNull();
}
boolean getBoolean() throws Exception {
return getInt() != 0;
}
int getInt() throws Exception {
if(isNull()) return 0;
switch(interval){
case SQLTokenizer.SQL_TSI_FRAC_SECOND:
return (int)(param2.getLong() - param1.getLong());
case SQLTokenizer.SQL_TSI_SECOND:
return (int)(param2.getLong() /1000 - param1.getLong() /1000);
case SQLTokenizer.SQL_TSI_MINUTE:
return (int)(param2.getLong() /60000 - param1.getLong() /60000);
case SQLTokenizer.SQL_TSI_HOUR:
return (int)(param2.getLong() /3600000 - param1.getLong() /3600000);
case SQLTokenizer.SQL_TSI_DAY:
return (int)(param2.getLong() /86400000 - param1.getLong() /86400000);
case SQLTokenizer.SQL_TSI_WEEK:{
long day2 = param2.getLong() /86400000;
long day1 = param1.getLong() /86400000;
return (int)((day2 + 3) / 7 - (day1 + 3) / 7);
}case SQLTokenizer.SQL_TSI_MONTH:{
DateTime.Details details2 = new DateTime.Details(param2.getLong());
DateTime.Details details1 = new DateTime.Details(param1.getLong());
return (details2.year * 12 + details2.month) - (details1.year * 12 + details1.month);
}
case SQLTokenizer.SQL_TSI_QUARTER:{
DateTime.Details details2 = new DateTime.Details(param2.getLong());
DateTime.Details details1 = new DateTime.Details(param1.getLong());
return (details2.year * 4 + details2.month / 3) - (details1.year * 4 + details1.month / 3);
}
case SQLTokenizer.SQL_TSI_YEAR:{
DateTime.Details details2 = new DateTime.Details(param2.getLong());
DateTime.Details details1 = new DateTime.Details(param1.getLong());
return details2.year - details1.year;
}
default: throw new Error();
}
}
long getLong() throws Exception {
return getInt();
}
float getFloat() throws Exception {
return getInt();
}
double getDouble() throws Exception {
return getInt();
}
long getMoney() throws Exception {
return getInt() * 10000L;
}
MutableNumeric getNumeric() throws Exception {
if(isNull()) return null;
return new MutableNumeric(getInt());
}
Object getObject() throws Exception {
if(isNull()) return null;
return Utils.getInteger(getInt());
}
String getString() throws Exception {
if(isNull()) return null;
return String.valueOf(getInt());
}
int getDataType() {
return SQLTokenizer.INT;
}
}
package smallsql.junit;
import junit.framework.*;
import java.sql.*;
public class TestExceptions extends BasicTestCase {
private TestValue testValue;
private static boolean init;
private static final int SYNTAX = 1;
private static final int RUNTIME= 2;
private static final TestValue[] TESTS = new TestValue[]{
a( "01000",    0, SYNTAX,  "SELECT 23 FROM"), 
a( "01000",    0, SYNTAX,  "SELECT c FROM exceptions Group By i"), 
a( "01000",    0, SYNTAX,  "SELECT first(c) FROM exceptions Group By i ORDER  by c"), 
a( "01000",    0, SYNTAX,  "SELECT 1 ORDER BY substring('qwert', 2, -3)"), 
a( "01000",    0, RUNTIME, "SELECT abs('abc')"), 
a( "01000",    0, SYNTAX,  "Create Table anyTable (c char(10)"), 
a( "01000",    0, SYNTAX,  "SELECT {ts 'abc'}"), 
a( "01000",    0, RUNTIME, "SELECT cast('abc' as timestamp)"), 
a( "01000",    0, SYNTAX, "SELECT 0xas"), 
a( "01000",    0, RUNTIME, "SELECT cast('1234-56as' as uniqueidentifier)"), 
a( "01000",    0, SYNTAX, "SELECT {ts '2020-04-31 00:00:00.000'}"), 
a( "01000",    0, SYNTAX, "SELECT {ts '2020-02-30 12:30:15.000'}"), 
a( "01000",    0, SYNTAX, "SELECT {d '2021-02-29'}"), 
a( "01000",    0, SYNTAX, "SELECT {d '2021-22-09'}"), 
a( "01000",    0, SYNTAX, "SELECT {t '24:30:15.000'}"), 
a( "01000",    0, SYNTAX, "SELECT {t '12:60:15.000'}"), 
a( "01000",    0, SYNTAX, "SELECT {t '12:30:65.000'}"), 
a( "01000",    0, SYNTAX,  "SELECT * FROM exceptions JOIN"), 
a( "01000",    0, SYNTAX,  "select 10/2,"),
};
TestExceptions(TestValue testValue){
super(testValue.sql);
this.testValue = testValue;
}
private void init() throws Exception{
if(init) return;
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
dropTable( con, "exceptions");
st.execute("Create Table exceptions (c varchar(30), i int)");
init = true;
}
public void runTest() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = null;
try{
rs = st.executeQuery( testValue.sql );
}catch(SQLException sqle){
assertTrue( "There should no syntax error:"+sqle, SYNTAX == testValue.errorType);
assertSQLException( testValue.sqlstate, testValue.errorCode, sqle );
}
if(testValue.errorType == SYNTAX){
assertNull("There should be a syntax error", rs);
return;
}
try{
while(rs.next()){
for(int i=1; i<=rs.getMetaData().getColumnCount(); i++){
rs.getObject(i);
}
}
fail("There should be a runtime error");
}catch(SQLException sqle){
assertSQLException( testValue.sqlstate, testValue.errorCode, sqle );
}
}
public static Test suite() throws Exception{
TestSuite theSuite = new TestSuite("Exceptions");
for(int i=0; i<TESTS.length; i++){
theSuite.addTest(new TestExceptions( TESTS[i] ) );
}
return theSuite;
}
private static TestValue a(String sqlstate, int errorCode, int errorType, String sql ){
TestValue value = new TestValue();
value.sql       = sql;
value.sqlstate  = sqlstate;
value.errorCode = errorCode;
value.errorType = errorType;
return value;
}
private static class TestValue{
String sql;
String sqlstate;
int errorCode;
int errorType;
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
abstract class Command {
int type;
String catalog;
String name;
SSResultSet rs;
int updateCount = -1;
Expressions params  = new Expressions();
final Logger log;
Command(Logger log){
this.log = log;
this.columnExpressions = new Expressions();
}
Command(Logger log, Expressions columnExpressions){
this.log = log;
this.columnExpressions = columnExpressions;
}
void addColumnExpression( Expression column ) throws SQLException{
columnExpressions.add( column );
}
void addParameter( ExpressionValue param ){
params.add( param );
}
void verifyParams() throws SQLException{
for(int p=0; p<params.size(); p++){
if(((ExpressionValue)params.get(p)).isEmpty())
throw SmallSQLException.create(Language.PARAM_EMPTY, new Integer(p+1));
}
}
void clearParams(){
for(int p=0; p<params.size(); p++){
((ExpressionValue)params.get(p)).clear();
}
}
private ExpressionValue getParam(int idx) throws SQLException{
if(idx < 1 || idx > params.size())
throw SmallSQLException.create(Language.PARAM_IDX_OUT_RANGE, new Object[] { new Integer(idx), new Integer(params.size())});
return ((ExpressionValue)params.get(idx-1));
}
void setParamValue(int idx, Object value, int dataType) throws SQLException{
getParam(idx).set( value, dataType );
if(log.isLogging()){
log.println("param"+idx+'='+value+"; type="+dataType);
}
}
void setParamValue(int idx, Object value, int dataType, int length) throws SQLException{
getParam(idx).set( value, dataType, length );
if(log.isLogging()){
log.println("param"+idx+'='+value+"; type="+dataType+"; length="+length);
}
}
final void execute(SSConnection con, SSStatement st) throws SQLException{
int savepoint = con.getSavepoint();
try{
executeImpl( con, st );
}catch(Throwable e){
con.rollback(savepoint);
throw SmallSQLException.createFromException(e);
}finally{
if(con.getAutoCommit()) con.commit();
}
}
abstract void executeImpl(SSConnection con, SSStatement st) throws Exception;
SSResultSet getQueryResult() throws SQLException{
if(rs == null)
throw SmallSQLException.create(Language.RSET_NOT_PRODUCED);
return rs;
}
SSResultSet getResultSet(){
return rs;
}
int getUpdateCount(){
return updateCount;
}
boolean getMoreResults(){
rs = null;
updateCount = -1;
return false;
}
void setMaxRows(int max){
int getMaxRows(){return -1;}
}
package smallsql.database;
final class ExpressionFunctionOctetLen extends ExpressionFunctionReturnInt {
private static final int BYTES_PER_CHAR = 2;
final int getFunction() {
return SQLTokenizer.OCTETLEN;
}
boolean isNull() throws Exception {
return param1.isNull();
}
final int getInt() throws Exception {
if(isNull()) return 0;
String str = param1.getString();
return str.length() * BYTES_PER_CHAR;
}
}
package smallsql.database;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
abstract class TransactionStep{
FileChannel raFile;
TransactionStep(FileChannel raFile){
this.raFile = raFile;
}
abstract long commit() throws SQLException;
abstract void rollback() throws SQLException;
void freeLock(){
}
package smallsql.junit;
import java.io.File;
import java.sql.*;
public class TestExceptionMethods extends BasicTestCase{
public void testForwardOnly() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table ExceptionMethods(v varchar(30))");
con.createStatement().execute("Insert Into ExceptionMethods(v) Values('qwert')");
ResultSet rs = con.createStatement().executeQuery("Select * from ExceptionMethods");
assertEquals(true, rs.next());
try{
rs.isBeforeFirst();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.isFirst();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.first();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.previous();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.last();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.isLast();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.isAfterLast();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.afterLast();
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.absolute(1);
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
try{
rs.relative(1);
fail("SQLException 'ResultSet is forward only' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
}finally{
dropTable(con, "ExceptionMethods");
}
}
public void testGetConnection() throws Exception{
Connection con;
try{
con = DriverManager.getConnection(AllTests.JDBC_URL + "?abc");
con.close();
fail("SQLException should be thrown");
}catch(SQLException ex){
}
con = DriverManager.getConnection(AllTests.JDBC_URL + "? ");
con.close();
con = DriverManager.getConnection(AllTests.JDBC_URL + "?a=b; ; c=d  ; e = f; ; ");
Connection con2 = DriverManager.getConnection( "jdbc:smallsql:" + new File( AllTests.CATALOG ).getAbsolutePath());
con.close();
con2.close();
con = DriverManager.getConnection( "jdbc:smallsql:file:" + AllTests.CATALOG );
con.close();
}
public void testDuplicatedColumnCreate() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
try{
st.execute("Create Table DuplicatedColumn(col INT, Col INT)");
fail("SQLException 'Duplicated Column' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
}
public void testDuplicatedColumnAlter() throws Exception{
Connection con = AllTests.getConnection();
try{
Statement st = con.createStatement();
st.execute("Create Table DuplicatedColumn(col INT)");
try{
st.execute("ALTER TABLE DuplicatedColumn Add Col INT");
fail("SQLException 'Duplicated Column' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
}finally{
dropTable(con, "DuplicatedColumn");
}
}
public void testDuplicatedColumnInsert() throws Exception{
Connection con = AllTests.getConnection();
try{
Statement st = con.createStatement();
st.execute("Create Table DuplicatedColumn(col INT)");
try{
st.execute("INSERT INTO DuplicatedColumn(col,Col) Values(1,2)");
fail("SQLException 'Duplicated Column' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
}finally{
dropTable(con, "DuplicatedColumn");
}
}
public void testDuplicatedCreateTable() throws Exception{
Connection con = AllTests.getConnection();
try{
dropTable(con, "DuplicatedTable");
Statement st = con.createStatement();
st.execute("Create Table DuplicatedTable(col INT primary key)");
int tableFileCount = countFiles("DuplicatedTable");
try{
st.execute("Create Table DuplicatedTable(col INT primary key)");
fail("SQLException 'Duplicated Table' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
assertEquals("Additional Files created",tableFileCount, countFiles("DuplicatedTable"));
}finally{
dropTable(con, "DuplicatedTable");
}
}
private int countFiles(String fileNameStart){
int count = 0;
String names[] = new File(AllTests.CATALOG).list();
for(int i=0; i<names.length; i++){
if(names[i].startsWith(fileNameStart)){
count++;
}
}
return count;
}
public void testAmbiguousColumn() throws Exception{
Connection con = AllTests.getConnection();
try{
Statement st = con.createStatement();
st.execute("create table foo (myint number)");
st.execute("create table bar (myint number)");
try{
st.executeQuery("select myint from foo, bar");
fail("SQLException 'Ambiguous name' should be throw");
}catch(SQLException e){
assertSQLException("01000", 0, e);
}
}finally{
dropTable(con, "foo");
dropTable(con, "bar");
}
}
public void testClosedStatement() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.close();
try{
st.execute("Select 1");
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
try{
st.executeQuery("Select 1");
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
try{
st.executeUpdate("Select 1");
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
}
public void testClosedPreparedStatement() throws Exception{
Connection con = AllTests.getConnection();
PreparedStatement pr = con.prepareStatement("Select ?");
pr.setInt(1, 1);
pr.close();
try{
pr.setInt(1, 1);
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
try{
pr.execute();
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
try{
pr.executeQuery();
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
try{
pr.executeUpdate();
fail("Exception should throw");
}catch(SQLException ex){
assertSQLException("HY010", 0, ex);
}
}
}
package smallsql.database;
final class LongLongList {
private int size;
private long[] data;
LongLongList(){
this(16);
}
LongLongList(int initialSize){
data = new long[initialSize*2];
}
final int size(){
return size;
}
final long get1(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
return data[idx << 1];
}
final long get2(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
return data[(idx << 1) +1];
}
final void add(long value1, long value2){
int size2 = size << 1;
if(size2 >= data.length ){
resize(size2);
}
data[ size2   ] = value1;
data[ size2 +1] = value2;
size++;
}
final void clear(){
size = 0;
}
private final void resize(int newSize){
long[] dataNew = new long[newSize << 1];
System.arraycopy(data, 0, dataNew, 0, size << 1);
data = dataNew;
}
}
package smallsql.database;
final class MutableFloat extends Number implements Mutable{
float value;
MutableFloat(float value){
this.value = value;
}
public double doubleValue() {
return value;
}
public float floatValue() {
return value;
}
public int intValue() {
return (int)value;
}
public long longValue() {
return (long)value;
}
public String toString(){
return String.valueOf(value);
}
public Object getImmutableObject(){
return new Float(value);
}
}
package smallsql.database;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;
class Column implements Cloneable{
private Expression defaultValue = Expression.NULL; 
private String defaultDefinition; 
private String name;
private boolean identity;
private boolean caseSensitive;
private boolean nullable = true;
private int scale;
private int precision;
private int dataType;
private Identity counter; 
void setName( String name ){
this.name = name;
}
void setDefaultValue(Expression defaultValue, String defaultDefinition){
this.defaultValue 		= defaultValue;
this.defaultDefinition	= defaultDefinition;
}
Expression getDefaultValue(SSConnection con) throws SQLException{
if(identity)
counter.createNextValue(con);
return defaultValue;
}
String getDefaultDefinition(){
return defaultDefinition;
}
String getName(){
return name;
}
boolean isAutoIncrement(){
return identity;
}
void setAutoIncrement(boolean identity){
this.identity = identity;
}
int initAutoIncrement(FileChannel raFile, long filePos) throws IOException{
if(identity){
counter = new Identity(raFile, filePos);
defaultValue = new ExpressionValue( counter, SQLTokenizer.BIGINT );
}
return 8;
}
void setNewAutoIncrementValue(Expression obj) throws Exception{
if(identity){
counter.setNextValue(obj);
}
}
boolean isCaseSensitive(){
return caseSensitive;
}
void setNullable(boolean nullable){
this.nullable = nullable;
}
boolean isNullable(){
return nullable;
}
void setDataType(int dataType){
this.dataType = dataType;
}
int getDataType(){
return dataType;
}
int getDisplaySize(){
return SSResultSetMetaData.getDisplaySize( dataType, precision, scale);
}
void setScale(int scale){
this.scale = scale;
}
int getScale(){
switch(dataType){
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:
return scale;
default:
return Expression.getScale(dataType);
}
}
void setPrecision(int precision) throws SQLException{
if(precision<0) throw SmallSQLException.create(Language.COL_INVALID_SIZE, new Object[] { new Integer(precision), name});
this.precision = precision;
}
int getPrecision(){
return SSResultSetMetaData.getDataTypePrecision( dataType, precision );
}
int getColumnSize(){
if(SSResultSetMetaData.isNumberDataType(dataType))
return getPrecision();
else return getDisplaySize();
}
int getFlag(){
return (identity        ? 1 : 0) |
(caseSensitive   ? 2 : 0) |
(nullable        ? 4 : 0);
}
void setFlag(int flag){
identity        = (flag & 1) > 0;
caseSensitive   = (flag & 2) > 0;
nullable        = (flag & 4) > 0;
}
Column copy(){
try{
return (Column)clone();
}catch(Exception e){return null;}
}
}
package smallsql.database;
final class ExpressionFunctionLength extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.LENGTH;
}
final int getInt() throws Exception {
String str = param1.getString();
if(str == null) return 0;
int length = str.length();
while(length>=0 && str.charAt(length-1) == ' ') length--;
return length;
}
}
package smallsql.database;
final class ExpressionFunctionTruncate extends ExpressionFunctionReturnP1Number {
final int getFunction(){ return SQLTokenizer.TRUNCATE; }
boolean isNull() throws Exception{
return param1.isNull() || param2.isNull();
}
final double getDouble() throws Exception{
if(isNull()) return 0;
final int places = param2.getInt();
double value = param1.getDouble();
long factor = 1;
if(places > 0){
for(int i=0; i<places; i++){
factor *= 10;
}
value *= factor;
}else{
for(int i=0; i>places; i--){
factor *= 10;
}
value /= factor;
}
value -= value % 1; 
if(places > 0){
value /= factor;
}else{
value *= factor;
}
return value;
}
}
package smallsql.database;
class StorePageLink {
long filePos;
TableStorePage page;
StoreImpl getStore(Table table, SSConnection con, int lock) throws Exception{
TableStorePage page = this.page;
if(page == null)
return table.getStore( con, filePos, lock );
while(page.nextLock != null) page = page.nextLock;
return table.getStore( page, lock);
}
}
package smallsql.database;
final class ExpressionFunctionACos extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.ACOS; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.acos( param1.getDouble() );
}
}
package smallsql.database;
import java.util.Random;
final class ExpressionFunctionRand extends ExpressionFunctionReturnFloat {
final static private Random random = new Random();
final int getFunction(){ return SQLTokenizer.RAND; }
boolean isNull() throws Exception{
return getParams().length == 1 && param1.isNull();
}
final double getDouble() throws Exception{
if(getParams().length == 0)
return random.nextDouble();
if(isNull()) return 0;
return new Random(param1.getLong()).nextDouble();
}
}
package smallsql.database;
import java.sql.SQLException;
import smallsql.database.language.Language;
class IndexDescriptions {
private int size;
private IndexDescription[] data;
void create(SSConnection con, Database database, TableView tableView) throws Exception{
for(int i=0; i<size; i++){
data[i].create(con, database, tableView);
}
}
void drop(Database database) throws Exception {
for(int i=0; i<size; i++){
data[i].drop(database);
}
}
void close() throws Exception{
for(int i=0; i<size; i++){
data[i].close();
}
}
void add(IndexDescriptions indexes) throws SQLException {
for(int i=0; i<indexes.size; i++){
add(indexes.data[i]);
}
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class StoreNull extends Store {
private final long nextPagePos;
StoreNull(){
this(-1);
}
StoreNull(long nextPos){
nextPagePos = nextPos;
}
final boolean isNull(int offset) {
return true;
}
final boolean getBoolean(int offset, int dataType) throws Exception {
return false;
}
final byte[] getBytes(int offset, int dataType) throws Exception {
return null;
}
final double getDouble(int offset, int dataType) throws Exception {
return 0;
}
final float getFloat(int offset, int dataType) throws Exception {
return 0;
}
final int getInt(int offset, int dataType) throws Exception {
return 0;
}
final long getLong(int offset, int dataType) throws Exception {
return 0;
}
final long getMoney(int offset, int dataType) throws Exception {
return 0;
}
final MutableNumeric getNumeric(int offset, int dataType) throws Exception {
return null;
}
final Object getObject(int offset, int dataType) throws Exception {
return null;
}
final String getString(int offset, int dataType) throws Exception {
return null;
}
final void scanObjectOffsets(int[] offsets, int[] dataTypes) {
final int getUsedSize() {
return 0;
}
final long getNextPagePos(){
return nextPagePos;
}
final void deleteRow(SSConnection con) throws SQLException{
if(nextPagePos >= 0){
throw SmallSQLException.create(Language.ROW_DELETED);
}
throw new Error();
}
}
package smallsql.junit;
import junit.framework.*;
import java.math.*;
import java.sql.*;
public class TestFunctions extends BasicTestCase{
private TestValue testValue;
private static final String table = "table_functions";
private static final TestValue[] TESTS = new TestValue[]{
a("$3"               	, new BigDecimal("3.0000")),
a("$-3.1"              	, new BigDecimal("-3.1000")),
a("-$3.2"              	, new BigDecimal("-3.2000")),
a("1 + 2"               , new Integer(3)),
a("3 * 2"               , new Integer(6)),
a("Top 1 4 / 2"         , new Integer(2)),
a("7/3"         		, new Integer(2)),
a("5 - 2"               , new Integer(3)),
a("- aint"              , new Integer(120)),
a("5 - - 2"             , new Integer(7)),
a("5 - - - 2"           , new Integer(3)),
a("-.123E-1"            , new Double("-0.0123")),
a(".123E-1"             , new Double("0.0123")),
a("123e-1"              , new Double("12.3")),
a("123E1"               , new Double("1230")),
a("2*5+2"               , new Integer("12")),
a("'a''b'"              , "a'b"),
a("'a\"b'"              , "a\"b"),
a("~1"                  , new Integer(-2)),
a("abs(-5)"             , new Integer(5)),
a("abs(aint)"           , new Integer(120)),
a("abs("+table+".aint)" , new Integer(120)),
a("abs(null)"           , null),
a("abs(cast(5 as money))"  , new BigDecimal("5.0000")),
a("abs(cast(-5 as money))" , new BigDecimal("5.0000")),
a("abs(cast(-5 as numeric(4,2)))" , new BigDecimal("5.00")),
a("abs(cast(5 as real))"   , new Float(5)),
a("abs(cast(-5 as real))"  , new Float(5)),
a("abs(cast(-5 as float))" , new Double(5)),
a("abs(cast(5 as double))" , new Double(5)),
a("abs(cast(5 as smallint))",new Integer(5)),
a("abs(cast(-5 as bigint))", new Long(5)),
a("abs(cast(5 as bigint))",  new Long(5)),
a("convert(money, abs(-5))", new BigDecimal("5.0000")),
a("convert(varchar(30), 11)" 	, "11"),
a("convert(varchar(30), null)" 	, null),
a("convert(varchar(1), 12)" 	, "1"),
a("convert(char(5), 11)" 		, "11   "),
a("convert(longvarchar, {d '1999-10-12'})" 	, "1999-10-12"),
a("convert(binary(5), '11')" 	, new byte[]{'1','1',0,0,0}),
a("convert(binary(5), null)" 	, null),
a("convert(varbinary(5), 11)" 	, new byte[]{0,0,0,11}),
a("convert(longvarbinary, '11')", new byte[]{'1','1'}),
a("convert(varchar(30),convert(varbinary(30),'Meherban'))", "Meherban"),
a("convert(bit, 1)" 			, Boolean.TRUE),
a("convert(bit, false)" 		, Boolean.FALSE),
a("convert(boolean, 0)" 		, Boolean.FALSE),
a("convert(varchar(30), convert(bit, false))" 		, "0"),
a("convert(varchar(30), convert(boolean, 0))" 		, "false"),
a("convert(bigint, 11)" 		, new Long(11)),
a("convert(int, 11)" 			, new Integer(11)),
a("{fn convert(11, Sql_integer)}" 			, new Integer(11)),
a("convert(integer, 11)" 			, new Integer(11)),
a("convert(smallint, 123456789)", new Integer((short)123456789)),
a("convert(tinyint, 123456789)"	, new Integer(123456789 & 0xFF)),
a("convert(date, '1909-10-12')" , Date.valueOf("1909-10-12")),
a("convert(date, null)" 		, null),
a("convert(date, {ts '1999-10-12 15:14:13.123456'})" 	, Date.valueOf("1999-10-12")),
a("convert(date, now())" 		, Date.valueOf( new Date(System.currentTimeMillis()).toString()) ),
a("curdate()" 					, Date.valueOf( new Date(System.currentTimeMillis()).toString()) ),
a("current_date()" 				, Date.valueOf( new Date(System.currentTimeMillis()).toString()) ),
a("hour(curtime())" 			, new Integer(new Time(System.currentTimeMillis()).getHours()) ),
a("minute({t '10:11:12'})" 		, new Integer(11) ),
a("month( {ts '1899-10-12 15:14:13.123456'})" 	, new Integer(10)),
a("year({d '2004-12-31'})"    , new Integer(2004)),
a("convert(time, '15:14:13')" 	, Time.valueOf("15:14:13")),
a("convert(time, null)" 		, null),
a("convert(timestamp, '1999-10-12 15:14:13.123456')" 	, Timestamp.valueOf("1999-10-12 15:14:13.123")),
a("cast({ts '1907-06-05 04:03:02.1'} as smalldatetime)", Timestamp.valueOf("1907-06-05 04:03:00.0")),
a("cast({ts '2007-06-05 04:03:02.1'} as smalldatetime)", Timestamp.valueOf("2007-06-05 04:03:00.0")),
a("convert(varchar(30), {d '1399-10-12 3:14:13'},  -1)" 	, "1399-10-12"),
a("convert(varchar(30), {ts '1999-10-12  3:14:13.12'},  99)" 	, "1999-10-12 03:14:13.12"),
a("convert(varchar(30), {ts '1999-10-12  0:14:13.123456'},   0)" 	, getMonth3L(10) + " 12 1999 12:14AM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   1)" 	, "10/12/99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   2)" 	, "99.10.12"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   3)" 	, "12/10/99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   4)" 	, "12.10.99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   5)" 	, "12-10-99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   6)" 	, "12 " + getMonth3L(10) + " 99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   7)" 	, getMonth3L(10) + " 12, 99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   8)" 	, "15:14:13"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   9)" 	, getMonth3L(10) + " 12 1999 03:14:13:123PM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  10)" 	, "10-12-99"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  11)" 	, "99/10/12"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  12)" 	, "991012"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  13)" 	, "12 " + getMonth3L(10) + " 1999 15:14:13:123"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  14)" 	, "15:14:13:123"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  20)" 	, "1999-10-12 15:14:13"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  21)" 	, "1999-10-12 15:14:13.123"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 100)" 	, getMonth3L(10) + " 12 1999 03:14PM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 101)" 	, "10/12/1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 102)" 	, "1999.10.12"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 103)" 	, "12/10/1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 104)" 	, "12.10.1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 105)" 	, "12-10-1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 106)" 	, "12 " + getMonth3L(10) + " 1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 107)" 	, getMonth3L(10) + " 12, 1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 108)" 	, "15:14:13"),
a("convert(varchar(30), {ts '1999-10-12  3:14:13.123456'}, 109)" 	, getMonth3L(10) + " 12 1999 03:14:13:123AM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 109)" 	, getMonth3L(10) + " 12 1999 03:14:13:123PM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 110)" 	, "10-12-1999"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 111)" 	, "1999/10/12"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 112)" 	, "19991012"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 113)" 	, "12 " + getMonth3L(10) + " 1999 15:14:13:123"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 114)" 	, "15:14:13:123"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 120)" 	, "1999-10-12 15:14:13"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 121)" 	, "1999-10-12 15:14:13.123"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 126)" 	, "1999-10-12T15:14:13.123"),
a("convert(varchar(30), {ts '1999-10-12  3:14:13.123456'}, 130)" 	, "12 " + getMonth3L(10) + " 1999 03:14:13:123AM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 130)" 	, "12 " + getMonth3L(10) + " 1999 03:14:13:123PM"),
a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 131)" 	, "12/10/99 15:14:13:123"),
a("convert(timestamp, null)" 	, null),
a("convert(real, 11)" 			, new Float(11)),
a("convert(real, null)" 		, null),
a("convert(float, 11.0)" 		, new Double(11)),
a("convert(double, '11')" 		, new Double(11)),
a("-convert(decimal, '11.123456')" 		, new BigDecimal("-11")),
a("-convert(decimal(38,6), '11.123456')" 		, new BigDecimal("-11.123456")),
a("convert(decimal(38,6), '11.123456') + 1" 		, new BigDecimal("12.123456")),
a("convert(decimal(38,6), '11.123456') - 1" 		, new BigDecimal("10.123456")),
a("convert(decimal(12,2), '11.0000') * 1" 		, new BigDecimal("11.00")),
a("convert(decimal(12,2), '11.0000') * convert(decimal(12,2), 1)" 		, new BigDecimal("11.0000")),
a("convert(decimal(12,2), '11.0000') / 1" 		, new BigDecimal("11.0000000")), 
a("convert(decimal(12,0), 11) / convert(decimal(12,2), 1)" 		, new BigDecimal("11.000000")), 
a("convert(money, -10000 / 10000.0)" 		, new BigDecimal("-1.0000")), 
a("-convert(money, '11.123456')" 		, new BigDecimal("-11.1235")),
a("-convert(smallmoney, '11.123456')" 	, new BigDecimal("-11.1235")),
a("convert(uniqueidentifier, 0x12345678901234567890)" 	, "78563412-1290-5634-7890-000000000000"),
a("convert(uniqueidentifier, '78563412-1290-5634-7890-000000000000')" 	, "78563412-1290-5634-7890-000000000000"),
a("convert(binary(16), convert(uniqueidentifier, 0x12345678901234567890))" 	, new byte[]{0x12,0x34,0x56,0x78,(byte)0x90,0x12,0x34,0x56,0x78,(byte)0x90,0,0,0,0,0,0}),
a("Timestampdiff(day,         {d '2004-10-12'}, {d '2004-10-14'})" 		, new Integer(2)),
a("Timestampdiff(SQL_TSI_DAY, {d '2004-10-12'}, {d '2004-10-15'})" 		, new Integer(3)),
a("Timestampdiff(d,           {d '2004-10-12'}, {d '2004-10-16'})" 		, new Integer(4)),
a("Timestampdiff(dd,          {d '2004-10-12'}, {d '2004-10-17'})" 		, new Integer(5)),
a("Timestampdiff(SQL_TSI_YEAR,{d '2000-10-12'}, {d '2005-10-17'})" 		, new Integer(5)),
a("Timestampdiff(year,			{d '2000-10-12'}, {d '2005-10-17'})" 		, new Integer(5)),
a("Timestampdiff(SQL_TSI_QUARTER,{d '2000-10-12'}, {d '2005-10-17'})" 	, new Integer(20)),
a("Timestampdiff(quarter,		{d '2000-10-12'}, {d '2005-10-17'})" 	, new Integer(20)),
a("Timestampdiff(SQL_TSI_MONTH,	{d '2004-10-12'}, {d '2005-11-17'})" 	, new Integer(13)),
a("Timestampdiff(month,			{d '2004-10-12'}, {d '2005-11-17'})" 	, new Integer(13)),
a("Timestampdiff(SQL_TSI_WEEK,	{d '2004-10-09'}, {d '2004-10-12'})" 		, new Integer(1)),
a("Timestampdiff(week,			{d '2004-10-09'}, {d '2004-10-12'})" 		, new Integer(1)),
a("Timestampdiff(SQL_TSI_HOUR,	{d '2004-10-12'}, {d '2004-10-13'})" 		, new Integer(24)),
a("Timestampdiff(hour,			{d '2004-10-12'}, {d '2004-10-13'})" 		, new Integer(24)),
a("Timestampdiff(SQL_TSI_MINUTE,{t '10:10:10'}, {t '11:11:11'})" 		, new Integer(61)),
a("Timestampdiff(minute,		{t '10:10:10'}, {t '11:11:11'})" 		, new Integer(61)),
a("Timestampdiff(SQL_TSI_SECOND,{t '00:00:10'}, {t '00:10:11'})" 		, new Integer(601)),
a("Timestampdiff(second,		{t '00:00:10'}, {t '00:10:11'})" 		, new Integer(601)),
a("Timestampdiff(SQL_TSI_FRAC_SECOND,{ts '2004-10-12 00:00:10.1'}, {ts '2004-10-12 00:00:10.2'})" 		, new Integer(100)),
a("Timestampdiff(millisecond,{ts '2004-10-12 00:00:10.1'}, {ts '2004-10-12 00:00:10.2'})" 		, new Integer(100)),
a("{fn TimestampAdd(SQL_TSI_YEAR,     1, {d '2004-10-17'})}" 		, Timestamp.valueOf("2005-10-17 00:00:00.0")),
a("{fn TimestampAdd(SQL_TSI_QUARTER,  1, {d '2004-10-17'})}"        , Timestamp.valueOf("2005-01-17 00:00:00.0")),
a("{fn TimestampAdd(SQL_TSI_MONTH,    1, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-11-17 00:00:00.0")),
a("{fn TimestampAdd(SQL_TSI_WEEK,     1, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-24 00:00:00.0")),
a("{fn TimestampAdd(SQL_TSI_HOUR,     1, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-17 01:00:00.0")),
a("{fn TimestampAdd(SQL_TSI_MINUTE,  61, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-17 01:01:00.0")),
a("{fn TimestampAdd(SQL_TSI_SECOND,  61, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-17 00:01:01.0")),
a("{fn TimestampAdd(SQL_TSI_FRAC_SECOND,1,{d '2004-10-17'})}"       , Timestamp.valueOf("2004-10-17 00:00:00.001")),
a("Timestampdiff(second, null, {t '00:10:11'})" 		, null),
a("Timestampdiff(second, {t '00:10:11'}, null)" 		, null),
a("TimestampAdd(year,     1, null)" 		, null),
a("DayOfWeek({d '2006-02-16'})" 		, new Integer(4)),
a("DayOfWeek({d '2006-02-19'})" 		, new Integer(7)),
a("DayOfYear({d '2004-01-01'})" 		, new Integer(1)),
a("DayOfYear({d '2004-02-29'})" 		, new Integer(60)),
a("DayOfYear({d '2004-03-01'})" 		, new Integer(61)),
a("DayOfYear({d '2004-12-31'})" 		, new Integer(366)),
a("DayOfMonth({d '1904-07-17'})" 		, new Integer(17)),
a("locate('ae', 'QWAERAE')"		, new Integer(3)),
a("locate('ae', 'QWAERAE', 3)"	, new Integer(3)),
a("locate('ae', 'QWAERAE', 4)"	, new Integer(6)),
a("locate('ae', 'QWAERAE', null)"		, new Integer(3)),
a("locate(null, 'QWAERAE', 4)"	, null),
a("locate('ae', null, 4)"	, null),
a("{d '2004-10-12'}"	, 				java.sql.Date.valueOf("2004-10-12")),
a("{ts '1999-10-12 15:14:13.123'}"	, 	Timestamp.valueOf("1999-10-12 15:14:13.123")),
a("{t '15:14:13'}"	, 					Time.valueOf("15:14:13")),
a("{fn length('abc')}", 				new Integer(3)),
a("{fn length('abc ')}", 				new Integer(3)),
a("{fn length(null)}", 					null),
a("{fn Right('qwertzu', 3)}", 			"tzu"),
a("{fn Right('qwertzu', 13)}", 			"qwertzu"),
a("cast( Right('1234', 2) as real)", 	new Float(34)),
a("cast( Right('1234', 2) as smallint)",new Integer(34)),
a("cast( Right('1234', 2) as boolean)", Boolean.TRUE),
a("right(0x1234567890, 2)",				new byte[]{0x78,(byte)0x90}),
a("right(null, 2)",						null),
a("left(null, 2)",                      null),
a("left('abcd', 2)",                    "ab"),
a("left(0x1234567890, 2)",              new byte[]{0x12,(byte)0x34}),
a("cast({fn SubString('ab2.3qw', 3, 3)} as double)", 	new Double(2.3)),
a("subString('qwert', 99, 2)", 		""),
a("{fn SubString(0x1234567890, 0, 99)}",new byte[]{0x12,0x34,0x56,0x78,(byte)0x90}),
a("{fn SubString(0x1234567890, 2, 2)}", new byte[]{0x34, 0x56}),
a("{fn SubString(0x1234567890, 99, 2)}", new byte[]{}),
a("SubString(null, 99, 2)", 			null),
a("Insert('abcd', 2, 1, 'qw')",         "aqwcd"),
a("Insert(0x1234, 2, 0, 0x56)",         new byte[]{0x12,0x56,0x34}),
a("STUFF(null, 2, 0, 0x56)",         	null),
a("lcase('Abcd')",                      "abcd"),
a("ucase('Abcd')",                      "ABCD"),
a("lcase(null)",                        null),
a("ucase(null)",                        null),
a("cast(1 as money) + SubString('a12', 2, 2)",new BigDecimal("13.0000")),
a("cast(1 as numeric(5,2)) + SubString('a12', 2, 2)",new BigDecimal("13.00")),
a("cast(1 as BigInt) + SubString('a12', 2, 2)",new Long(13)),
a("cast(1 as real) + SubString('a12', 2, 2)",new Float(13)),
a("1   + SubString('a12', 2, 2)",       new Integer(13)),
a("1.0 + SubString('a12', 2, 2)",       new Double(13)),
a("concat('abc', 'def')",               "abcdef"),
a("{fn IfNull(null, 'abc')}", 			"abc"),
a("{fn IfNull('asd', 'abc')}", 			"asd"),
a("iif(true, 1, 2)", 					new Integer(1)),
a("iif(false, 1, 2)", 					new Integer(2)),
a("CASE aVarchar WHEN 'qwert' THEN 25 WHEN 'asdfg' THEN 26 ELSE null END", new Integer(25)),
a("CASE WHEN aVarchar='qwert' THEN 'uu' WHEN aVarchar='bb' THEN 'gg' ELSE 'nn' END", "uu"),
a("{fn Ascii('')}", 			null),
a("{fn Ascii(null)}", 			null),
a("Ascii('abc')", 				new Integer(97)),
a("{fn Char(97)}", 				"a"),
a("Char(null)", 				null),
a("$1 + Char(49)",              new BigDecimal("2.0000")),
a("Exp(null)", 					null),
a("exp(0)", 					new Double(1)),
a("log(exp(2.4))", 				new Double(2.4)),
a("log10(10)", 					new Double(1)),
a("cos(null)", 					null),
a("cos(0)", 					new Double(1)),
a("acos(1)", 					new Double(0)),
a("sin(0)", 					new Double(0)),
a("cos(pi())", 					new Double(-1)),
a("asin(0)", 					new Double(0)),
a("asin(sin(0.5))",				new Double(0.5)),
a("tan(0)", 					new Double(0)),
a("atan(tan(0.5))",				new Double(0.5)),
a("atan2(0,3)",					new Double(0)),
a("atan2(0,-3)",				new Double(Math.PI)),
a("atn2(0,null)",				null),
a("cot(0)",						new Double(Double.POSITIVE_INFINITY)),
a("tan(0)", 					new Double(0)),
a("degrees(pi())", 				new Double(180)),
a("degrees(radians(50))", 		new Double(50)),
a("ceiling(123.45)", 			new Double(124)),
a("ceiling(-123.45)", 			new Double(-123)),
a("power(2, 3)", 				new Double(8)),
a("5.0 % 2", 					new Double(1)),
a("5 % 2", 						new Integer(1)),
a("mod(5, 2)", 					new Integer(1)),
a("FLOOR(123.45)", 				new Double(123)),
a("FLOOR('123.45')", 			new Double(123)),
a("FLOOR(-123.45)", 			new Double(-124)),
a("FLOOR($123.45)", 			new BigDecimal("123.0000")),
a("Rand(0)", 					new Double(0.730967787376657)),
a("ROUND(748.58, -4)", 			new Double(0)),
a("ROUND(-748.58, -2)", 		new Double(-700)),
a("ROUND('748.5876', 2)", 		new Double(748.59)),
a("round( 1e19, 0)"       , new Double(1e19)),
a("truncate( -1e19,0)"      , new Double(-1e19)),
a("Sign('748.5876')", 			new Integer(1)),
a("Sign(-2)", 					new Integer(-1)),
a("Sign(2)",                    new Integer(1)),
a("Sign(0)",                    new Integer(0)),
a("Sign(-$2)",                  new Integer(-1)),
a("Sign($2)",                   new Integer(1)),
a("Sign($0)",                   new Integer(0)),
a("Sign(cast(-2 as bigint))",   new Integer(-1)),
a("Sign(cast(2 as bigint))",    new Integer(1)),
a("Sign(cast(0 as bigint))",    new Integer(0)),
a("Sign(1.0)",                  new Integer(1)),
a("Sign(0.0)", 					new Integer(0)),
a("Sign(-.1)",                  new Integer(-1)),
a("Sign(cast(0 as numeric(5)))",new Integer(0)),
a("Sign(null)", 				null),
a("sqrt(9)", 					new Double(3)),
a("Truncate(748.58, -4)", 		new Double(0)),
a("Truncate(-748.58, -2)", 		new Double(-700)),
a("Truncate('748.5876', 2)", 	new Double(748.58)),
a("rtrim(null)",                null),
a("rtrim(0x0012345600)",        new byte[]{0x00,0x12,0x34,0x56}),
a("rtrim(' abc ')",             " abc"),
a("ltrim(null)",                null),
a("ltrim(0x0012345600)",        new byte[]{0x12,0x34,0x56,0x00}),
a("ltrim(' abc ')",             "abc "),
a("space(3)",                   "   "),
a("space(null)",                null),
a("space(-3)",                  null),
a("replace('abcabc','bc','4')", "a4a4"),
a("replace('abcabc','bc',null)",null),
a("replace('abcabc','','4')",   "abcabc"),
a("replace(0x123456,0x3456,0x77)", new byte[]{0x12,0x77}),
a("replace(0x123456,0x,0x77)",  new byte[]{0x12,0x34,0x56}),
a("replace(0x123456,0x88,0x77)",new byte[]{0x12,0x34,0x56}),
a("repeat('ab',4)",             "abababab"),
a("repeat(null,4)",             null),
a("repeat(0x1234,3)",           new byte[]{0x12,0x34,0x12,0x34,0x12,0x34}),
a("DIFFERENCE('Green','Greene')",new Integer(4)),
a("DIFFERENCE('Green',null)",   null),
a("OCTET_LENGTH('SomeWord')",   new Integer(16)),
a("OCTET_LENGTH('')",   		new Integer(0)),
a("OCTET_LENGTH(null)",   		null),
a("BIT_LENGTH('SomeWord')",     new Integer(128)),
a("BIT_LENGTH('')",   		    new Integer(0)),
a("BIT_LENGTH(null)",   		null),
a("CHAR_LENGTH('SomeWord')",    new Integer(8)),
a("CHAR_LENGTH('')",   		    new Integer(0)),
a("CHAR_LENGTH(null)",   		null),
a("CHARACTER_LENGTH('SomeWord')", new Integer(8)),
a("CHARACTER_LENGTH('')",   	new Integer(0)),
a("CHARACTER_LENGTH(null)",   	null),
a("soundex('Wikipedia')",       "W213"),
a("0x10 < 0x1020",              Boolean.TRUE),
};
private static TestValue a(String function, Object result){
TestValue value = new TestValue();
value.function  = function;
value.result    = result;
return value;
}
TestFunctions(TestValue testValue){
super(testValue.function);
this.testValue = testValue;
}
public void tearDown(){
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("drop table " + table);
st.close();
}catch(Throwable e){
}
}
public void setUp(){
tearDown();
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table " + table + "(aInt int, aVarchar varchar(100))");
st.execute("Insert into " + table + "(aInt, aVarchar) Values(-120,'qwert')");
st.close();
}catch(Throwable e){
e.printStackTrace();
}
}
public void runTest() throws Exception{
String query = "Select " + testValue.function + ",5 from " + table;
assertEqualsRsValue( testValue.result, query);
if(!testValue.function.startsWith("Top")){
assertEqualsRsValue( testValue.result, "Select " + testValue.function + " from " + table + " Group By " + testValue.function);
}
}
public static Test suite() throws Exception{
TestSuite theSuite = new TestSuite("Functions");
for(int i=0; i<TESTS.length; i++){
theSuite.addTest(new TestFunctions( TESTS[i] ) );
}
return theSuite;
}
private static class TestValue{
String function;
Object result;
}
}
package smallsql.database;
final class ExpressionFunctionHour extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.HOUR;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
DateTime.Details details = new DateTime.Details(param1.getLong());
return details.hour;
}
}
package smallsql.database;
import java.io.File;
class Lobs extends Table {
Lobs(Table table) throws Exception{
super(table.database, table.name);
raFile = Utils.openRaFile( getFile(database), database.isReadOnly() );
}
@Override
File getFile(Database database){
return new File( Utils.createLobFileName( database, name ) );
}
}
package smallsql.database;
class ForeignKeys {
private int size;
private ForeignKey[] data;
ForeignKeys(){
data = new ForeignKey[16];
}
final int size(){
return size;
}
final ForeignKey get(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
return data[idx];
}
final void add(ForeignKey foreignKey){
if(size >= data.length ){
resize(size << 1);
}
data[size++] = foreignKey;
}
private final void resize(int newSize){
ForeignKey[] dataNew = new ForeignKey[newSize];
System.arraycopy(data, 0, dataNew, 0, size);
data = dataNew;
}
}
package smallsql.database;
final class ExpressionFunctionSin extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.SIN; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.sin( param1.getDouble() );
}
}
package smallsql.database;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import smallsql.database.language.Language;
class SmallSQLException extends SQLException {
private static final long serialVersionUID = -1683756623665114L;
private boolean isInit;
private static Language language;
private SmallSQLException(String message, String vendorCode) {
super("[SmallSQL]" + message, vendorCode, 0);
init();
}
private SmallSQLException(Throwable throwable, String message, String vendorCode) {
super("[SmallSQL]" + message, vendorCode, 0);
this.initCause(throwable);
init();
}
private void init(){
this.isInit = true;
PrintWriter pw = DriverManager.getLogWriter();
if(pw != null) this.printStackTrace(pw);
}
static void setLanguage(Object localeObj) throws SQLException {
if (language != null && localeObj == null) return;
if (localeObj == null) {
language = Language.getDefaultLanguage();
}
else {
language = Language.getLanguage(localeObj.toString());
}
}
public void printStackTrace(){
if(!isInit) return;
super.printStackTrace();
}
public void printStackTrace(PrintStream ps){
if(!isInit) return;
super.printStackTrace(ps);
}
public void printStackTrace(PrintWriter pw){
if(!isInit) return;
super.printStackTrace(pw);
}
static SQLException create( String messageCode ) {
assert (messageCode != null): "Fill parameters";
String message = translateMsg(messageCode, null);
String sqlState = language.getSqlState(messageCode);
return new SmallSQLException(message, sqlState);
}
static SQLException create( String messageCode, Object param0 ) {
String message = translateMsg(messageCode, new Object[] { param0 });
String sqlState = language.getSqlState(messageCode);
return new SmallSQLException(message, sqlState);
}
static SQLException create( String messageCode, Object[] params ) {
String message = translateMsg(messageCode, params);
String sqlState = language.getSqlState(messageCode);
return new SmallSQLException(message, sqlState);
}
static SQLException createFromException( Throwable e ){
if(e instanceof SQLException) {
return (SQLException)e;
}
else {
String message = stripMsg(e);
String sqlState = language.getSqlState(Language.CUSTOM_MESSAGE);
return new SmallSQLException(e, message, sqlState);
}
}
static SQLException createFromException( String messageCode, Object param0,
Throwable e )
{
String message = translateMsg(messageCode, new Object[] { param0 });
String sqlState = language.getSqlState(messageCode);
return new SmallSQLException(e, message, sqlState);
}
static String translateMsg(String messageCode, Object[] params) {
assert ( messageCode != null && params != null ): "Fill parameters. msgCode=" + messageCode + " params=" + params;
String localized = language.getMessage(messageCode);
return MessageFormat.format(localized, params);
}
private static String stripMsg(Throwable throwable) {
String msg = throwable.getMessage();
if(msg == null || msg.length() < 30){
String msg2 = throwable.getClass().getName();
msg2 = msg2.substring(msg2.lastIndexOf('.')+1);
if(msg != null)
msg2 = msg2 + ':' + msg;
return msg2;
}
return throwable.getMessage();
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class CommandSelect extends Command{
private DataSources tables; 
private Expression where;
RowSource from;
private Expressions groupBy;
private Expression having;
private Expressions orderBy;
private boolean isAggregateFunction;
private int maxRows = -1;
final boolean isGroupResult(){
return groupBy != null || having != null || isAggregateFunction;
}
private void compileJoin( Join singleJoin ) throws Exception{
if(singleJoin.condition != null) compileLinkExpressionParams( singleJoin.condition );
if(singleJoin.left instanceof Join){
compileJoin( (Join)singleJoin.left );
}
if(singleJoin.right instanceof Join){
compileJoin( (Join)singleJoin.right );
}
}
private void compileLinkExpression( Expression expr) throws Exception{
if(expr.getType() == Expression.NAME)
compileLinkExpressionName( (ExpressionName)expr);
else compileLinkExpressionParams( expr );
}
private void compileLinkExpressionName(ExpressionName expr) throws Exception{
String tableAlias = expr.getTableAlias();
if(tableAlias != null){
int t = 0;
for(; t < tables.size(); t++){
DataSource fromEntry = tables.get(t);
if(tableAlias.equalsIgnoreCase(fromEntry.getAlias())){
TableView table = fromEntry.getTableView();
int colIdx = table.findColumnIdx(expr.getName());
if(colIdx >= 0){
expr.setFrom(fromEntry, colIdx, table);
break;
}else
throw SmallSQLException.create(Language.COL_INVALID_NAME, new Object[]{expr.getName()});
}
}
if(t == tables.size())
throw SmallSQLException.create(Language.COL_WRONG_PREFIX, tableAlias);
}else{
boolean isSetFrom = false;
for(int t = 0; t < tables.size(); t++){
DataSource fromEntry = tables.get(t);
TableView table = fromEntry.getTableView();
int colIdx = table.findColumnIdx(expr.getName());
if(colIdx >= 0){
if(isSetFrom){
throw SmallSQLException.create(Language.COL_AMBIGUOUS, expr.getName());
}
isSetFrom = true;
expr.setFrom(fromEntry, colIdx, table);
}
}
if(!isSetFrom){
throw SmallSQLException.create(Language.COL_INVALID_NAME, expr.getName());
}
}
compileLinkExpressionParams(expr);
}
private void compileLinkExpressionParams(Expression expr) throws Exception{
Expression[] expParams = expr.getParams();
isAggregateFunction = isAggregateFunction || expr.getType() >= Expression.GROUP_BEGIN;
if(expParams != null){
for(int k=0; k<expParams.length; k++){
Expression param = expParams[k];
int paramType = param.getType();
isAggregateFunction = isAggregateFunction || paramType >= Expression.GROUP_BEGIN;
if(paramType == Expression.NAME)
compileLinkExpressionName( (ExpressionName)param );
else compileLinkExpressionParams( param );
}
}
expr.optimize();
}
private final int compileAdd_All_Table_Columns( DataSource fromEntry, TableView table, int position){
for(int k=0; k<table.columns.size(); k++){
ExpressionName expr = new ExpressionName( table.columns.get(k).getName() );
expr.setFrom( fromEntry, k, table );
columnExpressions.add( position++, expr );
}
return position;
}
void executeImpl(SSConnection con, SSStatement st) throws Exception{
compile(con);
if((st.rsType == ResultSet.TYPE_SCROLL_INSENSITIVE || st.rsType == ResultSet.TYPE_SCROLL_SENSITIVE) &&
!from.isScrollable()){
from = new Scrollable(from);
}
from.execute();
rs =  new SSResultSet( st, this );
}
void beforeFirst() throws Exception{
from.beforeFirst();
}
boolean isBeforeFirst() throws SQLException{
return from.isBeforeFirst();
}
boolean isFirst() throws SQLException{
return from.isFirst();
}
boolean first() throws Exception{
return from.first();
}
boolean previous() throws Exception{
return from.previous();
}
boolean next() throws Exception{
if(maxRows >= 0 && from.getRow() >= maxRows){
from.afterLast();
return false;
}
return from.next();
}
final boolean last() throws Exception{
if(maxRows >= 0){
if(maxRows == 0){
from.beforeFirst();
return false;
}
return from.absolute(maxRows);
}
return from.last();
}
final void afterLast() throws Exception{
from.afterLast();
}
boolean isLast() throws Exception{
return from.isLast();
}
boolean isAfterLast() throws Exception{
return from.isAfterLast();
}
final boolean absolute(int row) throws Exception{
return from.absolute(row);
}
final boolean relative(int rows) throws Exception{
return from.relative(rows);
}
final int getRow() throws Exception{
int row = from.getRow();
if(maxRows >= 0 && row > maxRows) return 0;
return row;
}
final void updateRow(SSConnection con, Expression[] newRowSources) throws SQLException{
int savepoint = con.getSavepoint();
try{
for(int t=0; t<tables.size(); t++){
TableViewResult result = TableViewResult.getTableViewResult( tables.get(t) );
TableView table = result.getTableView();
Columns tableColumns = table.columns;
int count = tableColumns.size();
Expression[] updateValues = new Expression[count];
boolean isUpdateNeeded = false;
for(int i=0; i<columnExpressions.size(); i++){
Expression src = newRowSources[i];
if(src != null && (!(src instanceof ExpressionValue) || !((ExpressionValue)src).isEmpty())){
Expression col = columnExpressions.get(i);
if(!col.isDefinitelyWritable())
throw SmallSQLException.create(Language.COL_READONLY, new Integer(i));
ExpressionName exp = (ExpressionName)col;
if(table == exp.getTable()){
updateValues[exp.getColumnIndex()] = src;
isUpdateNeeded = true;
continue;
}
}
}
if(isUpdateNeeded){
result.updateRow(updateValues);
}
}
}catch(Throwable e){
con.rollback(savepoint);
throw SmallSQLException.createFromException(e);
}finally{
if(con.getAutoCommit()) con.commit();
}
}
final void insertRow(SSConnection con, Expression[] newRowSources) throws SQLException{
if(tables.size() > 1)
throw SmallSQLException.create(Language.JOIN_INSERT);
if(tables.size() == 0)
throw SmallSQLException.create(Language.INSERT_WO_FROM);
int savepoint = con.getSavepoint();
try{
TableViewResult result = TableViewResult.getTableViewResult( tables.get(0) );
TableView table = result.getTableView();
Columns tabColumns = table.columns;
int count = tabColumns.size();
Expression[] updateValues = new Expression[count];
if(newRowSources != null){
for(int i=0; i<columnExpressions.size(); i++){
Expression src = newRowSources[i];
if(src != null && (!(src instanceof ExpressionValue) || !((ExpressionValue)src).isEmpty())){
Expression rsColumn = columnExpressions.get(i); 
if(!rsColumn.isDefinitelyWritable())
throw SmallSQLException.create(Language.COL_READONLY, new Integer(i));
ExpressionName exp = (ExpressionName)rsColumn;
if(table == exp.getTable()){
updateValues[exp.getColumnIndex()] = src;
continue;
}
}
updateValues[i] = null;
}
}
result.insertRow(updateValues);
}catch(Throwable e){
con.rollback(savepoint);
throw SmallSQLException.createFromException(e);
}finally{
if(con.getAutoCommit()) con.commit();
}
}
final void deleteRow(SSConnection con) throws SQLException{
int savepoint = con.getSavepoint();
try{
if(tables.size() > 1)
throw SmallSQLException.create(Language.JOIN_DELETE);
if(tables.size() == 0)
throw SmallSQLException.create(Language.DELETE_WO_FROM);
TableViewResult.getTableViewResult( tables.get(0) ).deleteRow();
}catch(Throwable e){
con.rollback(savepoint);
throw SmallSQLException.createFromException(e);
}finally{
if(con.getAutoCommit()) con.commit();
}
}
public int findColumn(String columnName) throws SQLException {
Expressions columns = columnExpressions;
for(int i=0; i<columns.size(); i++){
if(columnName.equalsIgnoreCase(columns.get(i).getAlias()))
return i;
}
throw SmallSQLException.create(Language.COL_MISSING, columnName);
}
final void setDistinct(boolean distinct){
this.isDistinct = distinct;
}
final void setSource(RowSource join){
this.from = join;
}
final void setTables( DataSources from ){
this.tables = from;
}
final void setWhere( Expression where ){
this.where = where;
}
final void setGroup(Expressions group){
this.groupBy = group;
}
final void setHaving(Expression having){
this.having = having;
}
final void setOrder(Expressions order){
this.orderBy = order;
}
final void setMaxRows(int max){
maxRows = max;
}
final int getMaxRows(){
return maxRows;
}
}
package smallsql.database;
class Where extends RowSource {
final private RowSource rowSource;
final private Expression where;
private int row = 0;
private boolean isCurrentRow;
Where(RowSource rowSource, Expression where){
this.rowSource = rowSource;
this.where = where;
}
RowSource getFrom(){
return rowSource;
}
final private boolean isValidRow() throws Exception{
return where == null || rowSource.rowInserted() || where.getBoolean();
}
final boolean isScrollable() {
return rowSource.isScrollable();
}
final boolean isBeforeFirst(){
return row == 0;
}
final boolean isFirst(){
return row == 1 && isCurrentRow;
}
final boolean isLast() throws Exception{
if(!isCurrentRow) return false;
long rowPos = rowSource.getRowPosition();
boolean isNext = next();
rowSource.setRowPosition(rowPos);
return !isNext;
}
final boolean isAfterLast(){
return row > 0 && !isCurrentRow;
}
final void beforeFirst() throws Exception {
rowSource.beforeFirst();
row = 0;
}
final boolean first() throws Exception {
isCurrentRow = rowSource.first();
while(isCurrentRow && !isValidRow()){
isCurrentRow = rowSource.next();
}
row = 1;
return isCurrentRow;
}
final boolean previous() throws Exception {
boolean oldIsCurrentRow = isCurrentRow;
do{
isCurrentRow = rowSource.previous();
}while(isCurrentRow && !isValidRow());
if(oldIsCurrentRow || isCurrentRow) row--;
return isCurrentRow;
}
final boolean next() throws Exception {
boolean oldIsCurrentRow = isCurrentRow;
do{
isCurrentRow = rowSource.next();
}while(isCurrentRow && !isValidRow());
if(oldIsCurrentRow || isCurrentRow) row++;
return isCurrentRow;
}
final boolean last() throws Exception{
while(next()){
return previous();
}
final void afterLast() throws Exception {
while(next()){
}
final int getRow() throws Exception {
return isCurrentRow ? row : 0;
}
final long getRowPosition() {
return rowSource.getRowPosition();
}
final void setRowPosition(long rowPosition) throws Exception {
rowSource.setRowPosition(rowPosition);
}
final void nullRow() {
rowSource.nullRow();
row = 0;
}
final void noRow() {
rowSource.noRow();
row = 0;
}
final boolean rowInserted() {
return rowSource.rowInserted();
}
final boolean rowDeleted() {
return rowSource.rowDeleted();
}
final void execute() throws Exception{
rowSource.execute();
}
boolean isExpressionsFromThisRowSource(Expressions columns){
return rowSource.isExpressionsFromThisRowSource(columns);
}
}
package smallsql.junit;
import java.sql.*;
public class TestAlterTable2 extends BasicTestCase {
private final String table = "AlterTable2";
public void setUp(){
tearDown();
}
public void tearDown(){
try {
dropTable( AllTests.getConnection(), table );
} catch (SQLException ex) {
ex.printStackTrace();
}
}
public void testWithPrimaryKey() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table "+table+" (keyField varchar(2) primary key)");
st.execute("alter table "+table+" add anotherField varchar(4)");
ResultSet rs = st.executeQuery("Select * From " + table);
assertRSMetaData( rs, new String[]{"keyField", "anotherField"},  new int[]{Types.VARCHAR, Types.VARCHAR} );
rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
assertRowCount( 1, rs );
}
public void testAddPrimaryKey() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table "+table+" (a varchar(2))");
st.execute("alter table "+table+" add b varchar(4) primary key");
ResultSet rs = st.executeQuery("Select * From " + table);
assertRSMetaData( rs, new String[]{"a", "b"},  new int[]{Types.VARCHAR, Types.VARCHAR} );
rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
assertRowCount( 1, rs );
}
public void testAdd2PrimaryKeys() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table "+table+" (a varchar(2) primary key)");
try {
st.execute("alter table "+table+" add b varchar(4) primary key");
fail("2 primary keys are invalid");
} catch (SQLException ex) {
assertSQLException("01000",0, ex);
}
ResultSet rs = st.executeQuery("Select * From " + table);
assertRSMetaData( rs, new String[]{"a"},  new int[]{Types.VARCHAR} );
rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
assertRowCount( 1, rs );
}
public void testAdd2Keys() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table "+table+" (a varchar(2) unique)");
st.execute("alter table "+table+" add b varchar(4) primary key");
ResultSet rs = st.executeQuery("Select * From " + table);
assertRSMetaData( rs, new String[]{"a", "b"},  new int[]{Types.VARCHAR, Types.VARCHAR} );
rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
assertRowCount( 2, rs );
}
}
package smallsql.junit;
import java.sql.*;
public class TestIdentifer extends BasicTestCase {
public TestIdentifer(){
super();
}
public TestIdentifer(String arg0) {
super(arg0);
}
public void testQuoteIdentifer() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"QuoteIdentifer");
con.createStatement().execute("create table \"QuoteIdentifer\"(\"a\" int default 5)");
ResultSet rs = con.createStatement().executeQuery("SELECT tbl.* from \"QuoteIdentifer\" tbl");
assertEquals( "a", rs.getMetaData().getColumnName(1));
assertEquals( "QuoteIdentifer", rs.getMetaData().getTableName(1));
while(rs.next()){
}
dropTable(con,"QuoteIdentifer");
}
}
package smallsql.database;
interface Mutable {
Object getImmutableObject();
}
package smallsql.database;
abstract class ExpressionFunctionReturnP1StringAndBinary extends ExpressionFunctionReturnP1 {
final boolean getBoolean() throws Exception {
if(isNull()) return false;
return Utils.string2boolean(getString().trim());
}
final int getInt() throws Exception {
if(isNull()) return 0;
return Integer.parseInt(getString().trim());
}
final long getLong() throws Exception {
if(isNull()) return 0;
return Long.parseLong(getString().trim());
}
final float getFloat() throws Exception {
if(isNull()) return 0;
return Float.parseFloat(getString().trim());
}
final double getDouble() throws Exception {
if(isNull()) return 0;
return Double.parseDouble(getString().trim());
}
final long getMoney() throws Exception {
if(isNull()) return 0;
return Money.parseMoney(getString().trim());
}
final MutableNumeric getNumeric() throws Exception {
if(isNull()) return null;
return new MutableNumeric(getString().trim());
}
final Object getObject() throws Exception {
if(SSResultSetMetaData.isBinaryDataType(param1.getDataType()))
return getBytes();
return getString();
}
}
package smallsql.database;
import smallsql.database.language.Language;
final class UnionAll extends DataSource {
private final DataSources dataSources = new DataSources();
private int dataSourceIdx;
private DataSource currentDS;
private int row;
void addDataSource(DataSource ds){
dataSources.add(ds);
currentDS = dataSources.get(0);
}
boolean init(SSConnection con) throws Exception{
boolean result = false;
int colCount = -1;
for(int i=0; i<dataSources.size(); i++){
DataSource ds = dataSources.get(i);
result |= ds.init(con);
int nextColCount = ds.getTableView().columns.size();
if(colCount == -1)
colCount = nextColCount;
else
if(colCount != nextColCount)
throw SmallSQLException.create(Language.UNION_DIFFERENT_COLS, new Object[] { new Integer(colCount), new Integer(nextColCount)});
}
return result;
}
final boolean isNull(int colIdx) throws Exception {
return currentDS.isNull(colIdx);
}
final boolean getBoolean(int colIdx) throws Exception {
return currentDS.getBoolean(colIdx);
}
final int getInt(int colIdx) throws Exception {
return currentDS.getInt(colIdx);
}
final long getLong(int colIdx) throws Exception {
return currentDS.getLong(colIdx);
}
final float getFloat(int colIdx) throws Exception {
return currentDS.getFloat(colIdx);
}
final double getDouble(int colIdx) throws Exception {
return currentDS.getDouble(colIdx);
}
final long getMoney(int colIdx) throws Exception {
return currentDS.getMoney(colIdx);
}
final MutableNumeric getNumeric(int colIdx) throws Exception {
return currentDS.getNumeric(colIdx);
}
final Object getObject(int colIdx) throws Exception {
return currentDS.getObject(colIdx);
}
final String getString(int colIdx) throws Exception {
return currentDS.getString(colIdx);
}
final byte[] getBytes(int colIdx) throws Exception {
return currentDS.getBytes(colIdx);
}
final int getDataType(int colIdx) {
return currentDS.getDataType(colIdx);
}
TableView getTableView(){
return currentDS.getTableView();
}
final boolean isScrollable(){
return false; 
}
final void beforeFirst() throws Exception {
dataSourceIdx = 0;
currentDS = dataSources.get(0);
currentDS.beforeFirst();
row = 0;
}
final boolean first() throws Exception {
dataSourceIdx = 0;
currentDS = dataSources.get(0);
boolean b = currentDS.first();
row = b ? 1 : 0;
return b;
}
final boolean next() throws Exception {
boolean n = currentDS.next();
row++;
if(n) return true;
while(dataSources.size() > dataSourceIdx+1){
currentDS = dataSources.get(++dataSourceIdx);
currentDS.beforeFirst();
n = currentDS.next();
if(n) return true;
}
row = 0;
return false;
}
final void afterLast() throws Exception {
dataSourceIdx = dataSources.size()-1;
currentDS = dataSources.get(dataSourceIdx);
currentDS.afterLast();
row = 0;
}
final int getRow() throws Exception {
return row;
}
private final int getBitCount(){
int size = dataSources.size();
int bitCount = 0;
while(size>0){
bitCount++;
size >>= 1;
}
return bitCount;
}
final long getRowPosition() {
int bitCount = getBitCount();
return dataSourceIdx | currentDS.getRowPosition() << bitCount;
}
final void setRowPosition(long rowPosition) throws Exception {
int bitCount = getBitCount();
int mask = 0xFFFFFFFF >>> (32 - bitCount);
dataSourceIdx = (int)rowPosition & mask;
currentDS = dataSources.get(dataSourceIdx);
currentDS.setRowPosition( rowPosition >> bitCount );
}
final boolean rowInserted(){
return currentDS.rowInserted();
}
final boolean rowDeleted(){
return currentDS.rowDeleted();
}
final void nullRow() {
currentDS.nullRow();
row = 0;
}
final void noRow() {
currentDS.noRow();
row = 0;
}
final void execute() throws Exception{
for(int i=0; i<dataSources.size(); i++){
dataSources.get(i).execute();
}
}
}
package smallsql.database;
import java.sql.*;
import java.math.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Calendar;
import java.net.URL;
import smallsql.database.language.Language;
public class SSResultSet implements ResultSet {
SSResultSetMetaData metaData = new SSResultSetMetaData();
private CommandSelect cmd;
private boolean wasNull;
SSStatement st;
private boolean isUpdatable;
private boolean isInsertRow;
private ExpressionValue[] values;
private int fetchDirection;
private int fetchSize;
SSResultSet( SSStatement st, CommandSelect cmd ){
this.st = st;
metaData.columns = cmd.columnExpressions;
this.cmd = cmd;
isUpdatable = st != null && st.rsConcurrency == CONCUR_UPDATABLE && !cmd.isGroupResult();
}
public void close(){
st.con.log.println("ResultSet.close");
cmd = null;
}
public boolean wasNull(){
return wasNull;
}
public String getString(int columnIndex) throws SQLException {
try{
Object obj = getObject(columnIndex);
if(obj instanceof String || obj == null){
return (String)obj;
}
if(obj instanceof byte[]){
return "0x" + Utils.bytes2hex( (byte[])obj );
}
return getValue(columnIndex).getString();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public boolean getBoolean(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
return expr.getBoolean();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public byte getByte(int columnIndex) throws SQLException {
return (byte)getInt( columnIndex );
}
public short getShort(int columnIndex) throws SQLException {
return (short)getInt( columnIndex );
}
public int getInt(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
return expr.getInt();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public long getLong(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
return expr.getLong();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public float getFloat(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
return expr.getFloat();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public double getDouble(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
return expr.getDouble();
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
try{
MutableNumeric obj = getValue(columnIndex).getNumeric();
wasNull = obj == null;
if(wasNull) return null;
return obj.toBigDecimal(scale);
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public byte[] getBytes(int columnIndex) throws SQLException {
try{
byte[] obj = getValue(columnIndex).getBytes();
wasNull = obj == null;
return obj;
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Date getDate(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return DateTime.getDate( expr.getLong() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Time getTime(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return DateTime.getTime( expr.getLong() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Timestamp getTimestamp(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return DateTime.getTimestamp( expr.getLong() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public InputStream getAsciiStream(int columnIndex) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "getUnicodeStream");
}
public InputStream getBinaryStream(int columnIndex) throws SQLException {
return new ByteArrayInputStream(getBytes(columnIndex));
}
public String getString(String columnName) throws SQLException {
return getString( findColumn( columnName ) );
}
public boolean getBoolean(String columnName) throws SQLException {
return getBoolean( findColumn( columnName ) );
}
public byte getByte(String columnName) throws SQLException {
return getByte( findColumn( columnName ) );
}
public short getShort(String columnName) throws SQLException {
return getShort( findColumn( columnName ) );
}
public int getInt(String columnName) throws SQLException {
return getInt( findColumn( columnName ) );
}
public long getLong(String columnName) throws SQLException {
return getLong( findColumn( columnName ) );
}
public float getFloat(String columnName) throws SQLException {
return getFloat( findColumn( columnName ) );
}
public double getDouble(String columnName) throws SQLException {
return getDouble( findColumn( columnName ) );
}
public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
return getBigDecimal( findColumn( columnName ), scale );
}
public byte[] getBytes(String columnName) throws SQLException {
return getBytes( findColumn( columnName ) );
}
public Date getDate(String columnName) throws SQLException {
return getDate( findColumn( columnName ) );
}
public Time getTime(String columnName) throws SQLException {
return getTime( findColumn( columnName ) );
}
public Timestamp getTimestamp(String columnName) throws SQLException {
return getTimestamp( findColumn( columnName ) );
}
public InputStream getAsciiStream(String columnName) throws SQLException {
return getAsciiStream( findColumn( columnName ) );
}
public InputStream getUnicodeStream(String columnName) throws SQLException {
return getUnicodeStream( findColumn( columnName ) );
}
public InputStream getBinaryStream(String columnName) throws SQLException {
return getBinaryStream( findColumn( columnName ) );
}
public SQLWarning getWarnings(){
return null;
}
public void clearWarnings(){
}
public String getCursorName(){
return null;
}
public ResultSetMetaData getMetaData(){
return metaData;
}
public Object getObject(int columnIndex) throws SQLException {
try{
Object obj = getValue(columnIndex).getApiObject();
wasNull = obj == null;
return obj;
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Object getObject(String columnName) throws SQLException {
return getObject( findColumn( columnName ) );
}
public int findColumn(String columnName) throws SQLException {
return getCmd().findColumn(columnName) + 1;
}
public Reader getCharacterStream(int columnIndex) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Reader object");
}
public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
updateValue( columnIndex, x, -1);
}
public void updateObject(int columnIndex, Object x) throws SQLException {
updateValue( columnIndex, x, -1);
}
public void updateNull(String columnName) throws SQLException {
updateNull( findColumn( columnName ) );
}
public void updateBoolean(String columnName, boolean x) throws SQLException {
updateBoolean( findColumn( columnName ), x );
}
public void updateByte(String columnName, byte x) throws SQLException {
updateByte( findColumn( columnName ), x );
}
public void updateShort(String columnName, short x) throws SQLException {
updateShort( findColumn( columnName ), x );
}
public void updateInt(String columnName, int x) throws SQLException {
updateInt( findColumn( columnName ), x );
}
public void updateLong(String columnName, long x) throws SQLException {
updateLong( findColumn( columnName ), x );
}
public void updateFloat(String columnName, float x) throws SQLException {
updateFloat( findColumn( columnName ), x );
}
public void updateDouble(String columnName, double x) throws SQLException {
updateDouble( findColumn( columnName ), x );
}
public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
updateBigDecimal( findColumn( columnName ), x );
}
public void updateString(String columnName, String x) throws SQLException {
updateString( findColumn( columnName ), x );
}
public void updateBytes(String columnName, byte[] x) throws SQLException {
updateBytes( findColumn( columnName ), x );
}
public void updateDate(String columnName, Date x) throws SQLException {
updateDate( findColumn( columnName ), x );
}
public void updateTime(String columnName, Time x) throws SQLException {
updateTime( findColumn( columnName ), x );
}
public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
updateTimestamp( findColumn( columnName ), x );
}
public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
updateAsciiStream( findColumn( columnName ), x, length );
}
public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
updateBinaryStream( findColumn( columnName ), x, length );
}
public void updateCharacterStream(String columnName, Reader x, int length) throws SQLException {
updateCharacterStream( findColumn( columnName ), x, length );
}
public void updateObject(String columnName, Object x, int scale) throws SQLException {
updateObject( findColumn( columnName ), x, scale );
}
public void updateObject(String columnName, Object x) throws SQLException {
updateObject( findColumn( columnName ), x );
}
public void insertRow() throws SQLException {
st.con.log.println("insertRow()");
if(!isInsertRow){
throw SmallSQLException.create(Language.RSET_NOT_INSERT_ROW);
}
getCmd().insertRow( st.con, values);
clearRowBuffer();
}
private void testNotInsertRow() throws SQLException{
if(isInsertRow){
throw SmallSQLException.create(Language.RSET_ON_INSERT_ROW);
}
}
public void updateRow() throws SQLException {
try {
if(values == null){
return;
}
st.con.log.println("updateRow()");
testNotInsertRow();
final CommandSelect command = getCmd();
command.updateRow( st.con, values);
command.relative(0);  
clearRowBuffer();
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public void deleteRow() throws SQLException {
st.con.log.println("deleteRow()");
testNotInsertRow();
getCmd().deleteRow(st.con);
clearRowBuffer();
}
public void refreshRow() throws SQLException {
testNotInsertRow();
relative(0);
}
public void cancelRowUpdates() throws SQLException{
testNotInsertRow();
clearRowBuffer();
}
private void clearRowBuffer(){
if(values != null){
for(int i=values.length-1; i>=0; i--){
values[i].clear();
}
}
}
public void moveToInsertRow() throws SQLException {
if(isUpdatable){
isInsertRow = true;
clearRowBuffer();
}else{
throw SmallSQLException.create(Language.RSET_READONLY);
}
}
public void moveToCurrentRow() throws SQLException{
isInsertRow = false;
clearRowBuffer();
if(values == null){
getUpdateValue(1);
}
}
public Statement getStatement() {
return st;
}
public Object getObject(int i, Map map) throws SQLException {
return getObject( i );
}
public Ref getRef(int i) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Blob object");
}
public Clob getClob(int i) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Array");
}
public Object getObject(String columnName, Map map) throws SQLException {
return getObject( columnName );
}
public Ref getRef(String columnName) throws SQLException {
return getRef( findColumn( columnName ) );
}
public Blob getBlob(String columnName) throws SQLException {
return getBlob( findColumn( columnName ) );
}
public Clob getClob(String columnName) throws SQLException {
return getClob( findColumn( columnName ) );
}
public Array getArray(String columnName) throws SQLException {
return getArray( findColumn( columnName ) );
}
public Date getDate(int columnIndex, Calendar cal) throws SQLException {
try{
if(cal == null){
return getDate(columnIndex);
}
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return new Date(DateTime.addDateTimeOffset( expr.getLong(), cal.getTimeZone() ));
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Date getDate(String columnName, Calendar cal) throws SQLException {
return getDate( findColumn( columnName ), cal );
}
public Time getTime(int columnIndex, Calendar cal) throws SQLException {
try{
if(cal == null){
return getTime(columnIndex);
}
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return new Time(DateTime.addDateTimeOffset( expr.getLong(), cal.getTimeZone() ));
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Time getTime(String columnName, Calendar cal) throws SQLException {
return getTime( findColumn( columnName ), cal );
}
public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
try{
if(cal == null){
return getTimestamp(columnIndex);
}
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return new Timestamp(DateTime.addDateTimeOffset( expr.getLong(), cal.getTimeZone() ));
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
return getTimestamp( findColumn( columnName ), cal );
}
public URL getURL(int columnIndex) throws SQLException {
try{
Expression expr = getValue(columnIndex);
wasNull = expr.isNull();
if(wasNull) return null;
return new URL( expr.getString() );
}catch(Exception e){
throw SmallSQLException.createFromException( e );
}
}
public URL getURL(String columnName) throws SQLException {
return getURL( findColumn( columnName ) );
}
public void updateRef(int columnIndex, Ref x) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Blob");
}
public void updateBlob(String columnName, Blob x) throws SQLException {
updateBlob( findColumn( columnName ), x );
}
public void updateClob(int columnIndex, Clob x) throws SQLException {
throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Array");
}
public void updateArray(String columnName, Array x) throws SQLException {
updateArray( findColumn( columnName ), x );
}
final private Expression getValue(int columnIndex) throws SQLException{
if(values != null){
ExpressionValue value = values[ metaData.getColumnIdx( columnIndex ) ];
if(!value.isEmpty() || isInsertRow){
return value;
}
}
return metaData.getColumnExpression(columnIndex);
}
final private ExpressionValue getUpdateValue(int columnIndex) throws SQLException{
if(values == null){
int count = metaData.getColumnCount();
values = new ExpressionValue[count];
while(count-- > 0){
values[count] = new ExpressionValue();
}
}
return values[ metaData.getColumnIdx( columnIndex ) ];
}
final private void updateValue(int columnIndex, Object x, int dataType) throws SQLException{
getUpdateValue( columnIndex ).set( x, dataType );
if(st.con.log.isLogging()){
st.con.log.println("parameter '"+metaData.getColumnName(columnIndex)+"' = "+x+"; type="+dataType);
}
}
final private void updateValue(int columnIndex, Object x, int dataType, int length) throws SQLException{
getUpdateValue( columnIndex ).set( x, dataType, length );
if(st.con.log.isLogging()){
st.con.log.println("parameter '"+metaData.getColumnName(columnIndex)+"' = "+x+"; type="+dataType+"; length="+length);
}
}
final private CommandSelect getCmd() throws SQLException {
if(cmd == null){
throw SmallSQLException.create(Language.RSET_CLOSED);
}
st.con.testClosedConnection();
return cmd;
}
}
package smallsql.database;
abstract class ExpressionFunctionReturnString extends ExpressionFunction {
boolean isNull() throws Exception {
return param1.isNull();
}
final boolean getBoolean() throws Exception {
if(isNull()) return false;
return Utils.string2boolean(getString().trim());
}
final int getInt() throws Exception {
if(isNull()) return 0;
return Integer.parseInt(getString().trim());
}
final long getLong() throws Exception {
if(isNull()) return 0;
return Long.parseLong(getString().trim());
}
final float getFloat() throws Exception {
if(isNull()) return 0;
return Float.parseFloat(getString().trim());
}
final double getDouble() throws Exception {
if(isNull()) return 0;
return Double.parseDouble(getString().trim());
}
final long getMoney() throws Exception {
if(isNull()) return 0;
return Money.parseMoney(getString().trim());
}
final MutableNumeric getNumeric() throws Exception {
if(isNull()) return null;
return new MutableNumeric(getString().trim());
}
final Object getObject() throws Exception {
return getString();
}
}
package smallsql.database.language;
public class Language_de extends Language {
protected Language_de() {
addMessages(ENTRIES);
}
public String[][] getEntries() {
return ENTRIES;
}
private final String[][] ENTRIES = {
{ UNSUPPORTED_OPERATION           , "Nicht unterstützte Funktion: {0}" },
{ CANT_LOCK_FILE                  , "Die Datei ''{0}'' kann nicht gelockt werden. Eine einzelne SmallSQL Datenbank kann nur für einen einzigen Prozess geöffnet werden." },
{ DB_EXISTENT                     , "Die Datenbank ''{0}'' existiert bereits." },
{ DB_NONEXISTENT                  , "Die Datenbank ''{0}'' existiert nicht." },
{ DB_NOT_DIRECTORY                , "Das Verzeichnis ''{0}'' ist keine SmallSQL Datenbank." },
{ DB_NOTCONNECTED                 , "Sie sind nicht mit einer Datenbank verbunden." },
{ CONNECTION_CLOSED               , "Die Verbindung ist bereits geschlossen." },
{ VIEW_INSERT                     , "INSERT wird nicht unterstützt für eine View." },
{ VIEWDROP_NOT_VIEW               , "DROP VIEW kann nicht mit ''{0}'' verwendet werden, weil es keine View ist." },
{ VIEW_CANTDROP                   , "View ''{0}'' kann nicht gelöscht werden." },
{ RSET_NOT_PRODUCED               , "Es wurde kein ResultSet erzeugt." },
{ RSET_READONLY                   , "Das ResultSet ist schreibgeschützt." },
{ RSET_FWDONLY                    , "Das ResultSet ist forward only." },
{ RSET_CLOSED                     , "Das ResultSet ist geschlossen." },
{ RSET_NOT_INSERT_ROW             , "Der Cursor zeigt aktuell nicht auf die Einfügeposition (insert row)." },
{ RSET_ON_INSERT_ROW              , "Der Cursor zeigt aktuell auf die Einfügeposition (insert row)." },
{ ROWSOURCE_READONLY              , "Die Rowsource ist schreibgeschützt." },
{ STMT_IS_CLOSED                  , "Das Statement ist bereits geschlossen." },
{ SUBQUERY_COL_COUNT              , "Die Anzahl der Spalten in der Subquery muss 1 sein und nicht {0}." },
{ JOIN_DELETE                     , "Die Methode deleteRow wird nicht unterstützt für Joins." },
{ JOIN_INSERT                     , "Die Methode insertRow wird nicht unterstützt für Joins." },
{ DELETE_WO_FROM                  , "Die Methode deleteRow benötigt einen FROM Ausdruck." },
{ INSERT_WO_FROM                  , "Die Methode insertRow benötigt einen FROM Ausdruck." },
{ TABLE_CANT_RENAME               , "Die Tabelle ''{0}'' kann nicht umbenannt werden." },
{ TABLE_CANT_DROP                 , "Die Tabelle ''{0}'' kann nicht gelöscht werden." },
{ TABLE_CANT_DROP_LOCKED          , "Die Tabelle ''{0}'' kann nicht gelöscht werden, weil sie gelockt ist." },
{ TABLE_CORRUPT_PAGE              , "Beschädigte Tabellenseite bei Position: {0}." },
{ TABLE_MODIFIED                  , "Die Tabelle ''{0}'' wurde modifiziert." },
{ TABLE_DEADLOCK                  , "Deadlock, es kann kein Lock erzeugt werden für Tabelle ''{0}''." },
{ TABLE_OR_VIEW_MISSING           , "Tabelle oder View ''{0}'' existiert nicht." },
{ TABLE_FILE_INVALID              , "Die Datei ''{0}'' enthält keine gültige SmallSQL Tabelle." },
{ TABLE_OR_VIEW_FILE_INVALID      , "Die Datei ''{0}'' ist keine gültiger Tabellen oder View Speicher." },
{ TABLE_EXISTENT                  , "Die Tabelle oder View ''{0}'' existiert bereits." },
{ FK_NOT_TABLE                    , "''{0}'' ist keine Tabelle." },
{ PK_ONLYONE                      , "Eine Tabelle kann nur einen Primärschlüssel haben." },
{ KEY_DUPLICATE                   , "Doppelter Schlüssel." },
{ MONTH_TOOLARGE                  , "Der Monat ist zu groß im DATE oder TIMESTAMP Wert ''{0}''." },
{ DAYS_TOOLARGE                   , "Die Tage sind zu groß im DATE oder TIMESTAMP Wert ''{0}''." },
{ HOURS_TOOLARGE                  , "Die Stunden sind zu groß im TIME oder TIMESTAMP Wert ''{0}''." },
{ MINUTES_TOOLARGE                , "Die Minuten sind zu groß im TIME oder TIMESTAMP Wert ''{0}''." },
{ SECS_TOOLARGE                   , "Die Sekunden sind zu groß im TIME oder TIMESTAMP Wert ''{0}''." },
{ MILLIS_TOOLARGE                 , "Die Millisekunden sind zu groß im TIMESTAMP Wert ''{0}''." },
{ DATETIME_INVALID                , "''{0}'' ist ein ungültiges DATE, TIME or TIMESTAMP." },
{ UNSUPPORTED_CONVERSION_OPER     , "Nicht unterstützte Konvertierung zu Datentyp ''{0}'' von Datentyp ''{1}'' für die Operation ''{2}''." },
{ UNSUPPORTED_DATATYPE_OPER       , "Nicht unterstützter Datentyp ''{0}'' für Operation ''{1}''." },
{ UNSUPPORTED_DATATYPE_FUNC       , "Nicht unterstützter Datentyp ''{0}'' für Funktion ''{1}''." },
{ UNSUPPORTED_CONVERSION_FUNC     , "Nicht unterstützte Konvertierung zu Datentyp ''{0}'' für Funktion ''{1}''." },
{ UNSUPPORTED_TYPE_CONV           , "Nicht unterstützter Typ für CONVERT Funktion: {0}." },
{ UNSUPPORTED_TYPE_SUM            , "Nicht unterstützter Datentyp ''{0}'' für SUM Funktion." },
{ UNSUPPORTED_TYPE_MAX            , "Nicht unterstützter Datentyp ''{0}'' für MAX Funktion." },
{ UNSUPPORTED_CONVERSION          , "Kann nicht konvertieren ''{0}'' [{1}] zu ''{2}''." },
{ INSERT_INVALID_LEN              , "Ungültige Länge ''{0}'' in Funktion INSERT." },
{ SUBSTR_INVALID_LEN              , "Ungültige Länge ''{0}'' in Funktion SUBSTRING." },
{ VALUE_STR_TOOLARGE              , "Der String Wert ist zu groß für die Spalte." },
{ VALUE_BIN_TOOLARGE              , "Ein Binäre Wert mit Länge {0} ist zu groß für eine Spalte mit der Größe {1}." },
{ VALUE_NULL_INVALID              , "Null Werte sind ungültig für die Spalte ''{0}''." },
{ VALUE_CANT_CONVERT              , "Kann nicht konvertieren ein {0} Wert zu einem {1} Wert." },
{ BYTEARR_INVALID_SIZE            , "Ungültige Bytearray Große {0} für UNIQUEIDENFIER." },
{ LOB_DELETED                     , "Lob Objekt wurde gelöscht." },
{ PARAM_CLASS_UNKNOWN             , "Unbekante Parameter Klasse: ''{0}''." },
{ PARAM_EMPTY                     , "Parameter {0} ist leer." },
{ PARAM_IDX_OUT_RANGE             , "Parameter Index {0} liegt außerhalb des Gültigkeitsbereiches. Der Wert muss zwischen 1 und {1} liegen." },
{ COL_DUPLICATE                   , "Es gibt einen doppelten Spaltennamen: ''{0}''." },
{ COL_MISSING                     , "Spalte ''{0}'' wurde nicht gefunden." },
{ COL_VAL_UNMATCH                 , "Die Spaltenanzahl und Werteanzahl ist nicht identisch." },
{ COL_INVALID_SIZE                , "Ungültige Spaltengröße {0} für Spalte ''{1}''." },
{ COL_WRONG_PREFIX                , "Der Spaltenprefix ''{0}'' passt zu keinem Tabellennamen oder Aliasnamen in dieser Abfrage." },
{ COL_READONLY                    , "Die Spalte {0} ist schreibgeschützt." },
{ COL_INVALID_NAME                , "Ungültiger Spaltenname ''{0}''." },
{ COL_IDX_OUT_RANGE               , "Spaltenindex außerhalb des Gültigkeitsbereiches: {0}." },
{ COL_AMBIGUOUS                   , "Die Spalte ''{0}'' ist mehrdeutig." },
{ GROUP_AGGR_INVALID              , "Aggregatfunktion sind nicht erlaubt im GROUP BY Klausel: ({0})." },
{ GROUP_AGGR_NOTPART              , "Der Ausdruck ''{0}'' ist nicht Teil einer Aggregatfunktion oder GROUP BY Klausel." },
{ ORDERBY_INTERNAL                , "Interner Error mit ORDER BY." },
{ UNION_DIFFERENT_COLS            , "Die SELECT Teile des UNION haben eine unterschiedliche Spaltenanzahl: {0} und {1}." },
{ INDEX_EXISTS                    , "Index ''{0}'' existiert bereits." },
{ INDEX_MISSING                   , "Index ''{0}'' existiert nicht." },
{ INDEX_FILE_INVALID              , "Die Datei ''{0}'' ist kein gültiger Indexspeicher." },
{ INDEX_CORRUPT                   , "Error beim Laden des Index. Die Index Datei ist beschädigt. ({0})." },
{ INDEX_TOOMANY_EQUALS            , "Zu viele identische Einträge im Index." },
{ FILE_TOONEW                     , "Dateiversion ({0}) der Datei ''{1}'' ist zu neu für diese Laufzeitbibliothek." },
{ FILE_TOOOLD                     , "Dateiversion ({0}) der Datei ''{1}'' ist zu alt für diese Laufzeitbibliothek." },
{ FILE_CANT_DELETE                , "Datei ''{0}'' kann nicht gelöscht werden." },
{ ROW_0_ABSOLUTE                  , "Datensatz 0 ist ungültig für die Methode absolute()." },
{ ROW_NOCURRENT                   , "Kein aktueller Datensatz." },
{ ROWS_WRONG_MAX                  , "Fehlerhafter Wert für Maximale Datensatzanzahl: {0}." },
{ ROW_LOCKED                      , "Der Datensatz ist gelocked von einer anderen Verbindung." },
{ ROW_DELETED                     , "Der Datensatz ist bereits gelöscht." },
{ SAVEPT_INVALID_TRANS            , "Der Savepoint ist nicht gültig für die aktuelle Transaction." },
{ SAVEPT_INVALID_DRIVER           , "Der Savepoint ist nicht gültig für diesen Treiber {0}." },
{ ALIAS_UNSUPPORTED               , "Ein Alias ist nicht erlaubt für diesen Typ von Rowsource." },
{ ISOLATION_UNKNOWN               , "Unbekantes Transaktion Isolation Level: {0}." },
{ FLAGVALUE_INVALID               , "Ungültiger Wert des Flags in Methode getMoreResults: {0}." },
{ ARGUMENT_INVALID                , "Ungültiges Argument in Methode setNeedGenratedKeys: {0}." },
{ GENER_KEYS_UNREQUIRED           , "GeneratedKeys wurden nicht angefordert." },
{ SEQUENCE_HEX_INVALID            , "Ungültige Hexadecimal Sequenze bei Position {0}." },
{ SEQUENCE_HEX_INVALID_STR        , "Ungültige Hexadecimal Sequenze bei Position {0} in ''{1}''." },
{ SYNTAX_BASE_OFS                 , "Syntax Error bei Position {0} in ''{1}''. " },
{ SYNTAX_BASE_END                 , "Syntax Error, unerwartetes Ende des SQL Strings. " },
{ STXADD_ADDITIONAL_TOK           , "Zusätzliche Zeichen nach dem Ende des SQL statement." },
{ STXADD_IDENT_EXPECT             , "Bezeichner erwartet." },
{ STXADD_IDENT_EMPTY              , "Leerer Bezeichner." },
{ STXADD_IDENT_WRONG              , "Ungültiger Bezeichner ''{0}''." },
{ STXADD_OPER_MINUS               , "Ungültiger Operator Minus für Datentyp VARBINARY." },
{ STXADD_FUNC_UNKNOWN             , "Unbekannte Funktion." },
{ STXADD_PARAM_INVALID_COUNT      , "Ungültige Paramter Anzahl." },
{ STXADD_JOIN_INVALID             , "Ungültige Join Syntax." },
{ STXADD_FROM_PAR_CLOSE           , "Unerwartet schließende Klammer in FROM Klausel." },
{ STXADD_KEYS_REQUIRED            , "Benötige Schlüsselwörter sind: " },
{ STXADD_NOT_NUMBER               , "Eine Zahl ist erforderlich: ''{0}''." },
{ STXADD_COMMENT_OPEN             , "Fehlendes Kommentarende ''*/''." },
};
}
package smallsql.database;
final class ExpressionFunctionSqrt extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.SQRT; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.sqrt( param1.getDouble() );
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
abstract class ExpressionFunction extends Expression {
Expression param1;
Expression param2;
Expression param3;
Expression param4;
ExpressionFunction(){
super(FUNCTION);
}
abstract int getFunction();
byte[] getBytes() throws Exception{
return ExpressionValue.getBytes(getObject(), getDataType());
}
void setParams( Expression[] params ){
super.setParams( params );
if(params.length >0) param1 = params[0] ;
if(params.length >1) param2 = params[1] ;
if(params.length >2) param3 = params[2] ;
if(params.length >3) param4 = params[3] ;
}
final void setParamAt( Expression param, int idx){
switch(idx){
case 0:
param1 = param;
break;
case 1:
param2 = param;
break;
case 2:
param3 = param;
break;
case 3:
param4 = param;
break;
}
super.setParamAt( param, idx );
}
public boolean equals(Object expr){
if(!super.equals(expr)) return false;
if(!(expr instanceof ExpressionFunction)) return false;
return ((ExpressionFunction)expr).getFunction() == getFunction();
}
SQLException createUnspportedDataType( int dataType ){
Object[] params = {
SQLTokenizer.getKeyWord(dataType),
SQLTokenizer.getKeyWord(getFunction())
};
return SmallSQLException.create(Language.UNSUPPORTED_DATATYPE_FUNC, params);
}
SQLException createUnspportedConversion( int dataType ){
Object[] params = {
SQLTokenizer.getKeyWord(dataType),
SQLTokenizer.getKeyWord(getFunction())
};
return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION_FUNC, params);
}
}
package smallsql.database;
import java.sql.SQLException;
import java.util.ArrayList;
import smallsql.database.language.Language;
public class CommandInsert extends Command {
boolean noColumns; 
private CommandSelect cmdSel;
private Table table;
private long tableTimestamp;
private int[] matrix;  
CommandInsert(Logger log, String name){
super(log);
this.name = name;
}
void addColumnExpression(Expression column) throws SQLException{
if(columnExpressions.indexOf(column) >= 0){
throw SmallSQLException.create(Language.COL_DUPLICATE, column);
}
super.addColumnExpression(column);
}
void addValues(Expressions values){
this.cmdSel = new CommandSelect(log, values );
}
void addValues( CommandSelect cmdSel ){
this.cmdSel = cmdSel;
}
private void compile(SSConnection con) throws Exception{
TableView tableView = con.getDatabase(false).getTableView( con, name);
if(!(tableView instanceof Table))
throw SmallSQLException.create(Language.VIEW_INSERT);
table = (Table)tableView;
tableTimestamp = table.getTimestamp();
cmdSel.compile(con);
int count = table.columns.size();
matrix = new int[count];
if(noColumns){
columnExpressions.clear();
for(int i=0; i<count; i++){
matrix[i] = i;
}
if(count != cmdSel.columnExpressions.size())
throw SmallSQLException.create(Language.COL_VAL_UNMATCH);
}else{
for(int i=0; i<count; i++) matrix[i] = -1;
for(int c=0; c<columnExpressions.size(); c++){
Expression sqlCol = columnExpressions.get(c);
String sqlColName = sqlCol.getName();
int idx = table.findColumnIdx( sqlColName );
if(idx >= 0){
matrix[idx] = c;
}else{
throw SmallSQLException.create(Language.COL_MISSING, sqlColName);
}
}
if(columnExpressions.size() != cmdSel.columnExpressions.size())
throw SmallSQLException.create(Language.COL_VAL_UNMATCH);
}
}
void executeImpl(SSConnection con, SSStatement st) throws Exception {
if(table == null || tableTimestamp != table.getTimestamp()) compile( con );
final IndexDescriptions indexes = table.indexes;
updateCount = 0;
cmdSel.from.execute();
cmdSel.beforeFirst();
Strings keyColumnNames = null;
ArrayList keys = null;
boolean needGeneratedKeys = st.needGeneratedKeys();
int generatedKeysType = 0;
while(cmdSel.next()){
if(needGeneratedKeys){
keyColumnNames = new Strings();
keys = new ArrayList();
if(st.getGeneratedKeyNames() != null)
generatedKeysType = 1;
if(st.getGeneratedKeyIndexes() != null)
generatedKeysType = 2;
}
StoreImpl store = table.getStoreInsert( con );
for(int c=0; c<matrix.length; c++){
Column column = table.columns.get(c);
int idx = matrix[c];
Expression valueExpress;
if(idx >= 0){
valueExpress = cmdSel.columnExpressions.get(idx);
}else{
valueExpress = column.getDefaultValue(con);
if(needGeneratedKeys && generatedKeysType == 0 && valueExpress != Expression.NULL){
keyColumnNames.add(column.getName());
keys.add(valueExpress.getObject());
}
}
if(needGeneratedKeys && generatedKeysType == 1){
String[] keyNames = st.getGeneratedKeyNames();
for(int i=0; i<keyNames.length; i++){
if(column.getName().equalsIgnoreCase(keyNames[i])){
keyColumnNames.add(column.getName());
keys.add(valueExpress.getObject());
break;
}
}
}
if(needGeneratedKeys && generatedKeysType == 2){
int[] keyIndexes = st.getGeneratedKeyIndexes();
for(int i=0; i<keyIndexes.length; i++){
if(c+1 == keyIndexes[i]){
keyColumnNames.add(column.getName());
keys.add(valueExpress.getObject());
break;
}
}
}
store.writeExpression( valueExpress, column );
for(int i=0; i<indexes.size(); i++){
indexes.get(i).writeExpression( c, valueExpress );
}
}
store.writeFinsh( con );
for(int i=0; i<indexes.size(); i++){
indexes.get(i).writeFinish( con );
}
updateCount++;
if(needGeneratedKeys){
Object[][] data = new Object[1][keys.size()];
keys.toArray(data[0]);
st.setGeneratedKeys(new SSResultSet( st, Utils.createMemoryCommandSelect( con, keyColumnNames.toArray(), data)));
}
}
}
}
package smallsql.database;
final class ExpressionFunctionATan extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.ATAN; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.atan( param1.getDouble() );
}
}
package smallsql.database;
import java.io.ByteArrayOutputStream;
public class ExpressionFunctionReplace extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.REPLACE;
}
final boolean isNull() throws Exception {
return param1.isNull() || param2.isNull() || param3.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] str1 = param1.getBytes();
byte[] str2  = param2.getBytes();
int length = str2.length;
if(length == 0){
return str1;
}
byte[] str3  = param3.getBytes();
ByteArrayOutputStream buffer = new ByteArrayOutputStream();
int idx1 = 0;
int idx2 = Utils.indexOf(str2,str1,idx1);
while(idx2 > 0){
buffer.write(str1,idx1,idx2-idx1);
buffer.write(str3);
idx1 = idx2 + length;
idx2 = Utils.indexOf(str2,str1,idx1);
}
if(idx1 > 0){
buffer.write(str1,idx1,str1.length-idx1);
return buffer.toByteArray();
}
return str1;
}
final String getString() throws Exception {
if(isNull()) return null;
String str1 = param1.getString();
String str2  = param2.getString();
int length = str2.length();
if(length == 0){
return str1;
}
String str3  = param3.getString();
StringBuffer buffer = new StringBuffer();
int idx1 = 0;
int idx2 = str1.indexOf(str2,idx1);
while(idx2 >= 0){
buffer.append(str1.substring(idx1,idx2));
buffer.append(str3);
idx1 = idx2 + length;
idx2 = str1.indexOf(str2,idx1);
}
if(idx1 > 0){
buffer.append(str1.substring(idx1));
return buffer.toString();
}
return str1;
}
int getPrecision() {
return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
}
}
package smallsql.database;
import java.io.PrintWriter;
import java.sql.*;
class Logger {
boolean isLogging(){
return DriverManager.getLogWriter() != null;
}
void println(String msg){
PrintWriter log = DriverManager.getLogWriter();
if(log != null){
synchronized(log){
log.print("[Small SQL]");
log.println(msg);
log.flush();
}
}
}
}
package smallsql.junit;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
public class TestTokenizer extends BasicTestCase {
private static final String TABLE_NAME = "table_comments";
private static final PrintStream out = System.out;
private boolean init;
private Connection conn;
private Statement stat;
public void setUp() throws SQLException {
if (! init) {
conn = AllTests.createConnection("?locale=en", null);
stat = conn.createStatement();
init = true;
}
dropTable();
createTable();
}
public void tearDown() throws SQLException {
if (conn != null) {
dropTable();
stat.close();
conn.close();
}
}
private void createTable() throws SQLException {
stat.execute(
"CREATE TABLE " + TABLE_NAME +
" (id INT, myint INT)");
stat.execute(
"INSERT INTO " + TABLE_NAME + " VALUES (1, 2)");
stat.execute(
"INSERT INTO " + TABLE_NAME + " VALUES (1, 3)");
}
private void dropTable() throws SQLException {
try {
stat.execute("DROP TABLE " + TABLE_NAME);
} catch (SQLException e) {
out.println("REGULAR: " + e.getMessage() + '\n');
}
}
public void testSingleLine() throws SQLException {
final String SQL_1 =
"SELECT 10/2--mycomment\n" +
" , -- mycomment    \r\n" +
"id, SUM(myint)--my comment  \n\n" +
"FROM " + TABLE_NAME + " -- my other comment \r \r" +
"GROUP BY id --mycommentC\n" +
"--   myC    omment  E    \n" +
"ORDER BY id \r" +
"--myCommentD   \r\r\r";
successTest(SQL_1);
final String SQL_2 =
"SELECT 10/2 - - this must fail ";
failureTest(SQL_2, "Tokenized not-comment as a line-comment.");
}
public void testMultiLine() throws SQLException {
final String SQL_1 =
"SELECT 10/2, id, SUM(myint) 
" */ FROM 
" 
"
successTest(SQL_1);
final String SQL_2 =
"SELECT 10/2 / * this must fail */";
failureTest(SQL_2, "Tokenized not-comment as a multiline-comment.");
final String SQL_3 =
"SELECT 10/2 
failureTest(SQL_3,
"Uncomplete end multiline comment not recognized.",
"Missing end comment mark");
}
private void successTest(String sql) throws SQLException {
ResultSet rs_1 = stat.executeQuery(sql);
rs_1.next();
rs_1.close();
}
private void failureTest(String sql, String failureMessage) {
try {
stat.executeQuery(sql);
fail(failureMessage);
}
catch (SQLException e) {
out.println("REGULAR: " + e.getMessage() + '\n');
}
}
private void failureTest(String sql, String failureMessage, String expected) {
try {
stat.executeQuery(sql);
fail(failureMessage);
}
catch (SQLException e) {
String foundMsg = e.getMessage();
String assertMsg = MessageFormat.format(
"Unexpected error: [{0}], expected: [{1}]",
new Object[] { foundMsg, expected });
assertTrue(assertMsg, foundMsg.indexOf(expected) > -1);
out.println("REGULAR: " + e.getMessage() + '\n');
}
}
package smallsql.database;
public class ExpressionFunctionRTrim extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.RTRIM;
}
final boolean isNull() throws Exception {
return param1.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int length = bytes.length;
while(length>0 && bytes[length-1]==0){
length--;
}
byte[] b = new byte[length];
System.arraycopy(bytes, 0, b, 0, length);
return b;
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int length = str.length();
while(length>0 && str.charAt(length-1)==' '){
length--;
}
return str.substring(0,length);
}
}
package smallsql.junit;
import java.math.BigDecimal;
import java.sql.*;
public class TestGroupBy extends BasicTestCase {
private static final String table1 = "table_GroupBy1";
private static final String STR_VALUE1 = "name1";
private static final String STR_VALUE2 = "name2";
private boolean init;
public TestGroupBy() {
super();
}
public TestGroupBy(String name) {
super(name);
}
public void init(){
if(init) return;
try{
Connection con = AllTests.getConnection();
dropTable( con, table1 );
Statement st = con.createStatement();
st.execute("create table " + table1 + "(name varchar(30), id int )");
st.close();
PreparedStatement pr = con.prepareStatement("INSERT into " + table1 + "(name, id) Values(?,?)");
pr.setString( 1, STR_VALUE1);
pr.setInt( 2, 1 );
pr.execute();
pr.setString( 1, STR_VALUE1);
pr.setInt( 2, 2 );
pr.execute();
pr.setString( 1, STR_VALUE1);
pr.setNull( 2, Types.INTEGER );
pr.execute();
pr.setString( 1, STR_VALUE2);
pr.setInt( 2, 1 );
pr.execute();
pr.close();
init = true;
}catch(Throwable e){
e.printStackTrace();
}
}
public void testTest() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
rs = st.executeQuery("Select count(id) FROM " + table1 + " Group By name");
while(rs.next()){
rs.getObject(1);
}
rs = st.executeQuery("Select count(*) FROM " + table1 + " Group By name");
while(rs.next()){
rs.getObject(1);
}
rs = st.executeQuery("Select count(*) FROM " + table1);
assertTrue(rs.next());
assertEquals( 4, rs.getInt(1));
rs = st.executeQuery("Select count(id) FROM " + table1);
assertTrue(rs.next());
assertEquals( 3, rs.getInt(1));
rs = st.executeQuery("Select count(*)+1 FROM " + table1);
assertTrue(rs.next());
assertEquals( 5, rs.getInt(1));
}
public void testCountWhere() throws Exception{
init();
assertEqualsRsValue( new Integer(0), "Select count(*) FROM " + table1 + " Where id=-1234");
}
public void testCountWherePrepare() throws Exception{
init();
Connection con = AllTests.getConnection();
PreparedStatement pr = con.prepareStatement("Select count(*) FROM " + table1 + " Where id=-1234");
for(int i=1; i<=3; i++){
ResultSet rs = pr.executeQuery();
assertTrue( "No row produce in loop:"+i, rs.next());
assertEquals( "loop:"+i, 0, rs.getInt(1));
}
}
public void testCountOrderBy() throws Exception{
init();
Connection con = AllTests.getConnection();
PreparedStatement pr = con.prepareStatement("Select count(*) FROM " + table1 + " Group By name Order By name DESC");
for(int i=1; i<=3; i++){
ResultSet rs = pr.executeQuery( );
assertTrue  ( "loop:"+i, rs.next());
assertEquals( "loop:"+i, 1, rs.getInt(1));
assertTrue  ( "loop:"+i, rs.next());
assertEquals( "loop:"+i, 3, rs.getInt(1));
}
}
public void testGroupByWithExpression() throws Exception{
init();
Connection con = AllTests.getConnection();
PreparedStatement pr = con.prepareStatement("Select sum(id), name+'a' as ColumnName FROM " + table1 + " Group By name+'a' Order By Name+'a'");
for(int i=1; i<=3; i++){
ResultSet rs = pr.executeQuery( );
assertTrue  ( "loop:"+i, rs.next());
assertEquals( "loop:"+i, 3, rs.getInt(1));
assertTrue  ( "loop:"+i, rs.next());
assertEquals( "loop:"+i, 1, rs.getInt(1));
assertEquals( "loop:"+i+" Alias name von Expression", "ColumnName", rs.getMetaData().getColumnName(2));
}
}
public void testComplex() throws Exception{
init();
Connection con = AllTests.getConnection();
PreparedStatement pr = con.prepareStatement("Select abs(sum(abs(3-id))+2) FROM " + table1 + " Group By name+'a' Order By 'b'+(Name+'a')");
for(int i=1; i<=3; i++){
ResultSet rs = pr.executeQuery( );
assertTrue  ( "loop:"+i, rs.next());
assertEquals( "loop:"+i, 5, rs.getInt(1));
assertTrue  ( "loop:"+i, rs.next());
assertEquals( "loop:"+i, 4, rs.getInt(1));
}
}
public void testWithNullValue() throws Exception{
init();
assertEqualsRsValue(new Integer(4), "Select count(*) FROM " + table1 + " Group By name+null" );
}
public void testSumInt() throws Exception{
init();
assertEqualsRsValue( new Integer(4), "Select sum(id) FROM " + table1);
}
public void testSumLong() throws Exception{
init();
assertEqualsRsValue( new Long(4), "Select sum(cast(id as BigInt)) FROM " + table1);
}
public void testSumReal() throws Exception{
init();
assertEqualsRsValue( new Float(4), "Select sum(cast(id as real)) FROM " + table1);
}
public void testSumDouble() throws Exception{
init();
assertEqualsRsValue( new Double(4), "Select sum(cast(id as double)) FROM " + table1);
}
public void testSumDecimal() throws Exception{
init();
assertEqualsRsValue( new BigDecimal("4.00"), "Select sum(cast(id as decimal(38,2))) FROM " + table1);
}
public void testMaxInt() throws Exception{
init();
assertEqualsRsValue( new Integer(2), "Select max(id) FROM " + table1);
}
public void testMaxBigInt() throws Exception{
init();
assertEqualsRsValue( new Long(2), "Select max(cast(id as BigInt)) FROM " + table1);
}
public void testMaxString() throws Exception{
init();
assertEqualsRsValue( STR_VALUE2, "Select max(name) FROM " + table1);
}
public void testMaxTinyint() throws Exception{
init();
assertEqualsRsValue( new Integer(2), "Select max(convert(tinyint,id)) FROM " + table1);
}
public void testMaxReal() throws Exception{
init();
assertEqualsRsValue( new Float(2), "Select max(convert(real,id)) FROM " + table1);
}
public void testMaxFloat() throws Exception{
init();
assertEqualsRsValue( new Double(2), "Select max(convert(float,id)) FROM " + table1);
}
public void testMaxDouble() throws Exception{
init();
assertEqualsRsValue( new Double(2), "Select max(convert(double,id)) FROM " + table1);
}
public void testMaxMoney() throws Exception{
init();
assertEqualsRsValue( new java.math.BigDecimal("2.0000"), "Select max(convert(money,id)) FROM " + table1);
}
public void testMaxNumeric() throws Exception{
init();
assertEqualsRsValue( new java.math.BigDecimal("2"), "Select max(convert(numeric,id)) FROM " + table1);
}
public void testMaxDate() throws Exception{
init();
assertEqualsRsValue( java.sql.Date.valueOf("2345-01-23"), "Select max({d '2345-01-23'}) FROM " + table1);
}
public void testMaxTime() throws Exception{
init();
assertEqualsRsValue( java.sql.Time.valueOf("12:34:56"), "Select max({t '12:34:56'}) FROM " + table1);
}
public void testMaxTimestamp() throws Exception{
init();
assertEqualsRsValue( java.sql.Timestamp.valueOf("2345-01-23 12:34:56.123"), "Select max({ts '2345-01-23 12:34:56.123'}) FROM " + table1);
}
public void testMaxUniqueidentifier() throws Exception{
init();
String sql = "Select max(convert(uniqueidentifier, '12345678-3445-3445-3445-1234567890ab')) FROM " + table1;
assertEqualsRsValue( "12345678-3445-3445-3445-1234567890AB", sql);
}
public void testMaxOfNull() throws Exception{
init();
assertEqualsRsValue( null, "Select max(id) FROM " + table1 + " Where id is null");
}
public void testMin() throws Exception{
init();
assertEqualsRsValue( new Integer(1), "Select min(id) FROM " + table1);
}
public void testMinString() throws Exception{
init();
assertEqualsRsValue( STR_VALUE1, "Select min(name) FROM " + table1);
}
public void testMinOfNull() throws Exception{
init();
assertEqualsRsValue( null, "Select min(id) FROM " + table1 + " Where id is null");
}
public void testFirst1() throws Exception{
init();
assertEqualsRsValue( new Integer(1), "Select first(id) FROM " + table1);
}
public void testFirst2() throws Exception{
init();
assertEqualsRsValue( "name1", "Select first(name) FROM " + table1);
}
public void testLast1() throws Exception{
init();
assertEqualsRsValue( new Integer(1), "Select last(id) FROM " + table1);
}
public void testLast2() throws Exception{
init();
assertEqualsRsValue( "name2", "Select last(name) FROM " + table1);
}
public void testAvg() throws Exception{
init();
assertEqualsRsValue( new Integer(1), "Select avg(id) FROM " + table1);
}
public void testGroupBy() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
rs = st.executeQuery("Select name FROM " + table1 + " Group By name");
assertTrue(rs.next());
assertEquals( STR_VALUE1, rs.getObject(1) );
assertTrue(rs.next());
assertEquals( STR_VALUE2, rs.getObject(1) );
}
public void testViewWidthGroupBy() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
try{
ResultSet rs;
st.execute("Create View qry" + table1 + " as Select name, name as name2, count(*) as count FROM " + table1 + " Group By name");
rs = st.executeQuery("Select * from qry" + table1);
assertEquals( "name",  rs.getMetaData().getColumnLabel(1) );
assertEquals( "name2", rs.getMetaData().getColumnLabel(2) );
assertEquals( "count", rs.getMetaData().getColumnLabel(3) );
}finally{
st.execute("Drop View qry" + table1);
}
}
public void testCountNoRow() throws Exception{
init();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("Delete FROM " + table1);
init = false;
assertEqualsRsValue( new Integer(0), "Select count(*) FROM " + table1);
}
}
package smallsql.database;
import java.util.Arrays;
import smallsql.database.language.Language;
public class ExpressionFunctionConvert extends ExpressionFunction {
final private Column datatype;
public ExpressionFunctionConvert(Column datatype, Expression value, Expression style) {
super();
this.datatype = datatype;
Expression[] params = (style == null) ? new Expression[]{value} : new Expression[]{value, style};
setParams( params );
}
int getFunction() {
return SQLTokenizer.CONVERT;
}
boolean isNull() throws Exception {
return param1.isNull();
}
boolean getBoolean() throws Exception {
return ExpressionValue.getBoolean( getObject(), getDataType() );
}
int getInt() throws Exception {
return ExpressionValue.getInt( getObject(), getDataType() );
}
long getLong() throws Exception {
return ExpressionValue.getLong( getObject(), getDataType() );
}
float getFloat() throws Exception {
return ExpressionValue.getFloat( getObject(), getDataType() );
}
double getDouble() throws Exception {
return ExpressionValue.getDouble( getObject(), getDataType() );
}
long getMoney() throws Exception {
return ExpressionValue.getMoney(getObject(), getDataType());
}
MutableNumeric getNumeric() throws Exception {
return ExpressionValue.getNumeric(getObject(), getDataType());
}
String getString() throws Exception {
Object obj = getObject();
if(obj == null) return null;
switch(datatype.getDataType()){
case SQLTokenizer.BIT:
return ((Boolean)obj).booleanValue() ? "1" : "0";
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.LONGVARBINARY:
return new String( (byte[])obj );
}
return obj.toString();
}
Object getObject() throws Exception {
if(param1.isNull()) return null;
final int dataType = getDataType();
switch(dataType){
case SQLTokenizer.LONGVARCHAR:
return convertToString();
case SQLTokenizer.VARCHAR:{
String str = convertToString();
int length = datatype.getDisplaySize();
if(length > str.length())
return str;
return str.substring(0,length);
}
case SQLTokenizer.CHAR:{
String str = convertToString();
int length = datatype.getDisplaySize();
if(length > str.length()){
char[] buffer = new char[length-str.length()];
Arrays.fill(buffer, ' ');
return str + new String(buffer);
}
return str.substring(0,length);
}
case SQLTokenizer.LONGVARBINARY:
return param1.getBytes();
case SQLTokenizer.VARBINARY:{
byte[] bytes = param1.getBytes();
int length = datatype.getPrecision();
if(length < bytes.length){
byte[] buffer = new byte[length];
System.arraycopy(bytes, 0, buffer, 0, Math.min(bytes.length,length) );
return buffer;
}
return bytes;
}
case SQLTokenizer.BINARY:{
byte[] bytes = param1.getBytes();
int length = datatype.getPrecision();
if(length != bytes.length){
byte[] buffer = new byte[length];
System.arraycopy(bytes, 0, buffer, 0, Math.min(bytes.length,length) );
return buffer;
}
return bytes;
}
case SQLTokenizer.BOOLEAN:
case SQLTokenizer.BIT:
return param1.getBoolean() ? Boolean.TRUE : Boolean.FALSE;
case SQLTokenizer.TINYINT:
return Utils.getInteger(param1.getInt() & 0xFF);
case SQLTokenizer.SMALLINT:
return Utils.getInteger((short)param1.getInt());
case SQLTokenizer.INT:
return Utils.getInteger(param1.getInt());
case SQLTokenizer.BIGINT:
return new Long(param1.getLong());
case SQLTokenizer.REAL:
return new Float(param1.getFloat());
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return new Double(param1.getDouble());
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
return new DateTime( getDateTimeLong(), dataType );
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
MutableNumeric num = param1.getNumeric();
if(num != null && (dataType == SQLTokenizer.NUMERIC || dataType == SQLTokenizer.DECIMAL))
num.setScale(getScale());
return num;
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return Money.createFromUnscaledValue(param1.getMoney());
case SQLTokenizer.UNIQUEIDENTIFIER:
switch(param1.getDataType()){
case SQLTokenizer.VARCHAR:
case SQLTokenizer.CHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.CLOB:
return Utils.bytes2unique( Utils.unique2bytes(param1.getString()), 0);
}
return Utils.bytes2unique(param1.getBytes(), 0);
}
Object[] param = { SQLTokenizer.getKeyWord(dataType) };
throw SmallSQLException.create(Language.UNSUPPORTED_TYPE_CONV, param);
}
final private String convertToString() throws Exception{
if(param2 != null){
int type = param1.getDataType();
switch(type){
case SQLTokenizer.SMALLDATETIME:
type = SQLTokenizer.TIMESTAMP;
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
return new DateTime( param1.getLong(), type ).toString(param2.getInt());
default:
return param1.getString();
}
}else
return param1.getString();
}
final private long getDateTimeLong() throws Exception{
switch(param1.getDataType()){
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.CHAR:
return DateTime.parse( param1.getString() );
}
return param1.getLong();
}
final int getDataType() {
return datatype.getDataType();
}
final int getPrecision(){
final int dataType = getDataType();
switch(dataType){
case SQLTokenizer.VARCHAR:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.BINARY:
case SQLTokenizer.CHAR:
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return datatype.getPrecision();
default:
return super.getPrecision();
}
}
final int getScale() {
return datatype.getScale();
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
public class StoreNoCurrentRow extends Store {
private SQLException noCurrentRow(){
return SmallSQLException.create(Language.ROW_NOCURRENT);
}
boolean isNull(int offset) throws SQLException {
throw noCurrentRow();
}
boolean getBoolean(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
byte[] getBytes(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
double getDouble(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
float getFloat(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
int getInt(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
long getLong(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
long getMoney(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
MutableNumeric getNumeric(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
Object getObject(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
String getString(int offset, int dataType) throws Exception {
throw noCurrentRow();
}
void scanObjectOffsets(int[] offsets, int[] dataTypes) {
}
int getUsedSize() {
return 0;
}
long getNextPagePos(){
return -1;
}
void deleteRow(SSConnection con) throws SQLException{
throw noCurrentRow();
}
}
package smallsql.database;
class ExpressionFunctionAbs extends ExpressionFunctionReturnP1 {
int getFunction(){ return SQLTokenizer.ABS; }
boolean getBoolean() throws Exception{
return getDouble() != 0;
}
int getInt() throws Exception{
return Math.abs( param1.getInt() );
}
long getLong() throws Exception{
return Math.abs( param1.getLong() );
}
float getFloat() throws Exception{
return Math.abs( param1.getFloat() );
}
double getDouble() throws Exception{
return Math.abs( param1.getDouble() );
}
long getMoney() throws Exception{
return Math.abs( param1.getMoney() );
}
MutableNumeric getNumeric() throws Exception{
if(param1.isNull()) return null;
MutableNumeric num = param1.getNumeric();
if(num.getSignum() < 0) num.setSignum(1);
return num;
}
Object getObject() throws Exception{
if(param1.isNull()) return null;
Object para1 = param1.getObject();
switch(param1.getDataType()){
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
double dValue = ((Double)para1).doubleValue();
return (dValue<0) ? new Double(-dValue) : para1;
case SQLTokenizer.REAL:
double fValue = ((Float)para1).floatValue();
return (fValue<0) ? new Float(-fValue) : para1;
case SQLTokenizer.BIGINT:
long lValue = ((Number)para1).longValue();
return (lValue<0) ? new Long(-lValue) : para1;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
int iValue = ((Number)para1).intValue();
return (iValue<0) ? new Integer(-iValue) : para1;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
MutableNumeric nValue = (MutableNumeric)para1;
if(nValue.getSignum() <0) nValue.setSignum(1);
return nValue;
case SQLTokenizer.MONEY:
Money mValue = (Money)para1;
if(mValue.value <0) mValue.value = -mValue.value;
return mValue;
default: throw createUnspportedDataType(param1.getDataType());
}
}
String getString() throws Exception{
Object obj = getObject();
if(obj == null) return null;
return obj.toString();
}
}
package smallsql.database;
class ColumnExpression extends Column {
final private Expression expr;
ColumnExpression(Expression expr){
this.expr = expr;
}
String getName(){
return expr.getAlias();
}
boolean isAutoIncrement(){
return expr.isAutoIncrement();
}
boolean isCaseSensitive(){
return expr.isCaseSensitive();
}
boolean isNullable(){
return expr.isNullable();
}
int getDataType(){
return expr.getDataType();
}
int getDisplaySize(){
return expr.getDisplaySize();
}
int getScale(){
return expr.getScale();
}
int getPrecision(){
return expr.getPrecision();
}
}
package smallsql.database;
final class ExpressionFunctionATan2 extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.ATAN2; }
boolean isNull() throws Exception{
return param1.isNull() || param2.isNull();
}
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.atan2( param1.getDouble(), param2.getDouble() );
}
}
package smallsql.database;
final class MutableLong extends Number implements Mutable{
long value;
MutableLong(long value){
this.value = value;
}
public double doubleValue() {
return value;
}
public float floatValue() {
return value;
}
public int intValue() {
return (int)value;
}
public long longValue() {
return value;
}
public String toString(){
return String.valueOf(value);
}
public Object getImmutableObject(){
return new Long(value);
}
}
package smallsql.database;
class Strings {
private int size;
private String[] data;
Strings(){
data = new String[16];
}
final int size(){
return size;
}
final String get(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
return data[idx];
}
final void add(String descr){
if(size >= data.length ){
resize(size << 1);
}
data[size++] = descr;
}
private final void resize(int newSize){
String[] dataNew = new String[newSize];
System.arraycopy(data, 0, dataNew, 0, size);
data = dataNew;
}
public String[] toArray() {
String[] array = new String[size];
System.arraycopy(data, 0, array, 0, size);
return array;
}
}
package smallsql.database;
import java.io.*;
import smallsql.database.language.Language;
public class CommandCreateDatabase extends Command{
CommandCreateDatabase( Logger log, String name ){
super(log);
this.type = SQLTokenizer.DATABASE;
if(name.startsWith("file:"))
name = name.substring(5);
this.name = name;
}
@Override
void executeImpl(SSConnection con, SSStatement st) throws Exception{
if( con.isReadOnly() ){
throw SmallSQLException.create(Language.DB_READONLY);
}
File dir = new File( name );
dir.mkdirs();
if(!new File(dir, Utils.MASTER_FILENAME).createNewFile()){
throw SmallSQLException.create(Language.DB_EXISTENT, name);
}
}
}
package smallsql.database;
final class ExpressionFunctionRadians extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.RADIANS; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.toRadians( param1.getDouble() );
}
}
package smallsql.junit;
import junit.framework.*;
import java.math.BigDecimal;
import java.sql.*;
import smallsql.database.Money;
public class TestMoneyRounding extends TestCase{
static final String table = "TestMoneyRounding";
public void setUp() throws SQLException{
tearDown();
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table " + table + "(a money, b smallmoney)");
}
public void tearDown(){
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("drop table " + table);
st.close();
}catch(Throwable e){
}
}
public void testMoney1() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
int firstValue = -10000;
for(int i=firstValue; i<10000; i++){
st.execute("Insert into " + table + "(a,b) values(" + (i/10000.0) + "," +(i/10000.0) +")");
}
st.close();
verify(firstValue);
}
private void verify(int firstValue) throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery("Select * FROM " + table);
long i = firstValue;
while(rs.next()){
Object obj1 = rs.getObject(1);
Object obj2 = rs.getObject(2);
if(obj1 instanceof Money){
Money mon1 = (Money)obj1;
Money mon2 = (Money)obj2;
assertEquals("Roundungsfehler money:", i, mon1.unscaledValue());
assertEquals("Roundungsfehler smallmoney:", i, mon2.unscaledValue());
}else{
BigDecimal mon1 = (BigDecimal)obj1;
BigDecimal mon2 = (BigDecimal)obj2;
assertEquals("Roundungsfehler money:", i, mon1.unscaledValue().longValue());
assertEquals("Roundungsfehler smallmoney:", i, mon2.unscaledValue().longValue());
}
i++;
}
st.close();
}
public void testMoney2() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
int firstValue = -10000;
for(int i=firstValue; i<10000; i++){
st.execute("Insert into " + table + "(a,b) values( (" + i + "/10000.0), (" + i + "/10000.0) )");
}
st.close();
verify(firstValue);
}
}
package smallsql.database;
import java.sql.*;
abstract class Store {
static final Store NULL = new StoreNull();
static final Store NOROW= new StoreNoCurrentRow();
abstract boolean isNull(int offset) throws Exception;
abstract boolean getBoolean( int offset, int dataType) throws Exception;
abstract byte[] getBytes( int offset, int dataType) throws Exception;
abstract double getDouble( int offset, int dataType) throws Exception;
abstract float getFloat( int offset, int dataType) throws Exception;
abstract int getInt( int offset, int dataType) throws Exception;
abstract long getLong( int offset, int dataType) throws Exception;
abstract long getMoney( int offset, int dataType) throws Exception;
abstract MutableNumeric getNumeric( int offset, int dataType) throws Exception;
abstract Object getObject( int offset, int dataType) throws Exception;
abstract String getString( int offset, int dataType) throws Exception;
boolean isValidPage(){
return false;
}
abstract void scanObjectOffsets( int[] offsets, int dataTypes[] );
abstract int getUsedSize();
abstract long getNextPagePos();
abstract void deleteRow(SSConnection con) throws SQLException;
}
package smallsql.junit;
import java.sql.*;
public class BenchTest
{
static byte[] byteArray = {23, 34, 67 };
static byte[] largeByteArray = new byte[4000];
static String driverClassName = "smallsql.database.SSDriver";
static String userName        = "sa";
static String password        = "";
static String jdbcUrl         = "jdbc:smallsql:AllTests";
static int    rowCount        = 10000;
static Connection con;
static final String tableName = "BenchTest2";
public static void main(String[] args) throws SQLException{
for(int i=0; i<args.length;){
String option = args[i++];
if      (option.equals("-driver")  ) driverClassName = args[i++];
else if (option.equals("-user")    ) userName = args[i++];
else if (option.equals("-password")) password = args[i++];
else if (option.equals("-url")     ) jdbcUrl  = args[i++];
else if (option.equals("-rowcount")) rowCount = Integer.parseInt(args[i++]);
else if (option.equals("-?") | option.equals("-help")){
System.out.println( "Valid options are :\n\t-driver\n\t-url\n\t-user\n\t-password\n\t-rowcount");
System.exit(0);
}
else {System.out.println("Option " + option + " is ignored");i++;}
}
System.out.println( "Driver:  \t" + driverClassName);
System.out.println( "Username:\t" + userName);
System.out.println( "Password:\t" + password);
System.out.println( "JDBC URL:\t" + jdbcUrl);
System.out.println( "Row Count:\t" + rowCount);
System.out.println();
try{
Class.forName(driverClassName).newInstance();
con = DriverManager.getConnection( jdbcUrl, userName,password);
System.out.println( con.getMetaData().getDriverName() + " " + con.getMetaData().getDriverVersion());
System.out.println();
createTestTable( con );
test_InsertClassic( con );
test_DeleteAll( con );
test_InsertEmptyRows( con );
test_DeleteRows( con );
test_InsertRows( con );
test_RowRequestPages( con );
test_UpdateRows( con );
test_UpdateRowsPrepare( con );
test_UpdateRowsPrepareSP( con );
test_UpdateRowsPrepareBatch( con );
test_Scroll_getXXX( con );
test_UpdateLargeBinary( con );
test_UpdateLargeBinaryWithSP( con );
}catch(Exception e){
e.printStackTrace();
}finally{
if (con != null){
con.close();
}
}
}
static void test_InsertClassic(Connection con){
System.out.println();
System.out.println( "Test insert rows with default values with a classic insert statement: " + rowCount + " rows");
try{
Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
long time = -System.currentTimeMillis();
for (int i=0; i<rowCount; i++){
st.execute("INSERT INTO " + tableName + "(i) VALUES(" + i +")");
}
time += System.currentTimeMillis();
ResultSet rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
int count = rs.getInt(1);
if (count != rowCount)
System.out.println( "  Failed: Only " + count + " rows were inserted.");
else System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_DeleteAll(Connection con){
System.out.println();
System.out.println( "Test delete all rows: " + rowCount + " rows");
try{
long time = -System.currentTimeMillis();
Statement st = con.createStatement();
st.execute("DELETE FROM " + tableName);
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_InsertEmptyRows(Connection con){
System.out.println();
System.out.println( "Test insert empty rows with insertRow(): " + rowCount + " rows");
try{
Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
ResultSet rs = st.executeQuery("SELECT * FROM "+tableName);
long time = -System.currentTimeMillis();
for (int i=0; i<rowCount; i++){
rs.moveToInsertRow();
rs.insertRow();
}
time += System.currentTimeMillis();
rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
int count = rs.getInt(1);
if (count != rowCount)
System.out.println( "  Failed: Only " + count + " rows were inserted.");
else System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_DeleteRows(Connection con){
System.out.println();
System.out.println( "Test delete rows with deleteRow(): " + rowCount + " rows");
try{
Statement st1 = con.createStatement();
ResultSet rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
int count = rs.getInt(1);
if (count != rowCount){
if (count == 0){
createTestDataWithClassicInsert( con );
rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
count = rs.getInt(1);
}
if (count != rowCount){
System.out.println( "  Failed: Only " + (rowCount-count) + " rows were deleted.");
return;
}
}
st1.close();
Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
rs = st.executeQuery("SELECT * FROM "+tableName);
long time = -System.currentTimeMillis();
for (int i=0; i<rowCount; i++){
rs.next();
rs.deleteRow();
}
time += System.currentTimeMillis();
rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
count = rs.getInt(1);
if (count != 0)
System.out.println( "  Failed: Only " + (rowCount-count) + " rows were deleted.");
else System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_InsertRows(Connection con){
System.out.println();
System.out.println( "Test insert rows with insertRow(): " + rowCount + " rows");
try{
Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
long time = -System.currentTimeMillis();
for (int i=0; i<rowCount; i++){
rs.moveToInsertRow();
rs.updateBytes (  "bi", byteArray );
rs.updateString(  "c" , "Test" );
rs.updateDate  (  "d" , new Date( System.currentTimeMillis() ) );
rs.updateFloat (  "de", (float)1234.56789 );
rs.updateFloat (  "f" , (float)9876.54321 );
rs.updateBytes (  "im", largeByteArray );
rs.updateInt   (  "i" , i );
rs.updateDouble(  "m" , 23.45 );
rs.updateDouble(  "n" , 567.45 );
rs.updateFloat (  "r" , (float)78.89 );
rs.updateTime  (  "sd", new Time( System.currentTimeMillis() ) );
rs.updateShort (  "si", (short)i );
rs.updateFloat (  "sm", (float)34.56 );
rs.updateString(  "sy", "sysname (30) NULL" );
rs.updateString(  "t" , "ntext NULL, sample to save in the field" );
rs.updateByte  (  "ti", (byte)i );
rs.updateBytes (  "vb", byteArray );
rs.updateString(  "vc", "nvarchar (255) NULL" );
rs.insertRow();
}
time += System.currentTimeMillis();
rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
int count = rs.getInt(1);
if (count != rowCount){
st.execute("DELETE FROM " + tableName);
System.out.println( "  Failed: Only " + count + " rows were inserted.");
}else System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
e.printStackTrace();
try{
Statement st = con.createStatement();
st.execute("DELETE FROM " + tableName);
st.close();
}catch(Exception ee){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_RowRequestPages(Connection con){
int pages = 100;
int rows  = rowCount / pages;
System.out.println();
System.out.println( "Test request row pages : " + pages + " pages, " +rows + " rows per page");
try{
Statement st1 = con.createStatement();
ResultSet rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
int count = rs.getInt(1);
if (count != rowCount){
if (count == 0){
createTestDataWithClassicInsert( con );
rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
rs.next();
count = rs.getInt(1);
}
if (count != rowCount){
System.out.println( "  Failed: Only " + (rowCount-count) + " rows were found.");
return;
}
}
st1.close();
long time = -System.currentTimeMillis();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
st.setFetchSize( rows );
for (int i=0; i<pages; i++){
rs = st.executeQuery("SELECT * FROM " + tableName);
rs.absolute( i*rows+1 );
for (int r=1; r<rows; r++){
if (!rs.next()){
System.out.println( "  Failed: No rows were found at page " + i + " page and row " + r);
return;
}
int col_i = rs.getInt("i");
if (col_i != (i*rows+r)){
System.out.println( "  Failed: Wrong row " + col_i + ", it should be row " + (i*rows+r));
return;
}
}
}
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_UpdateRows(Connection con){
System.out.println();
System.out.println( "Test update rows with updateRow(): " + rowCount + " rows");
try{
Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
int colCount = rs.getMetaData().getColumnCount();
long time = -System.currentTimeMillis();
int count = 0;
while(rs.next()){
for (int i=2; i<=colCount; i++){
rs.updateObject( i, rs.getObject(i) );
}
rs.updateRow();
count++;
}
time += System.currentTimeMillis();
if (count != rowCount)
System.out.println( "  Failed: Only " + count + " rows were updated.");
else System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:" + e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_UpdateRowsPrepare(Connection con){
System.out.println();
System.out.println( "Test update rows with a PreparedStatement: " + rowCount + " rows");
try{
PreparedStatement pr = con.prepareStatement( "UPDATE " + tableName + " SET bi=?,c=?,d=?,de=?,f=?,im=?,i=?,m=?,n=?,r=?,sd=?,si=?,sm=?,sy=?,t=?,ti=?,vb=?,vc=? WHERE i=?" );
long time = -System.currentTimeMillis();
for (int i=0; i<rowCount; i++){
pr.setBytes (  1, byteArray );
pr.setString(  2 , "Test" );
pr.setDate  (  3 , new Date( System.currentTimeMillis() ) );
pr.setFloat (  4, (float)1234.56789 );
pr.setFloat (  5 , (float)9876.54321 );
pr.setBytes (  6, largeByteArray );
pr.setInt   (  7 , i );
pr.setDouble(  8 , 23.45 );
pr.setDouble(  9 , 567.45 );
pr.setFloat (  10 , (float)78.89 );
pr.setTime  (  11, new Time( System.currentTimeMillis() ) );
pr.setShort (  12, (short)23456 );
pr.setFloat (  13, (float)34.56 );
pr.setString(  14, "sysname (30) NULL" );
pr.setString(  15 , "text NULL" );
pr.setByte  (  16, (byte)28 );
pr.setBytes (  17, byteArray );
pr.setString(  18, "varchar (255) NULL" );
pr.setInt   (  19 , i );
int updateCount = pr.executeUpdate();
if (updateCount != 1){
System.out.println( "  Failed: Update count should be 1 but it is " + updateCount + ".");
return;
}
}
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
pr.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_UpdateRowsPrepareSP(Connection con){
System.out.println();
System.out.println( "Test update rows with a PreparedStatement and a stored procedure: " + rowCount + " rows");
try{
Statement st = con.createStatement();
try{st.execute("drop procedure sp_"+tableName);}catch(Exception e){
st.execute("create procedure sp_"+tableName+" (@bi binary,@c nchar(255),@d datetime,@de decimal,@f float,@im image,@i int,@m money,@n numeric(18, 0),@r real,@sd smalldatetime,@si smallint,@sm smallmoney,@sy sysname,@t ntext,@ti tinyint,@vb varbinary(255),@vc nvarchar(255)) as UPDATE " + tableName + " SET bi=@bi,c=@c,d=@d,de=@de,f=@f,im=@im,i=@i,m=@m,n=@n,r=@r,sd=@sd,si=@si,sm=@sm,sy=@sy,t=@t,ti=@ti,vb=@vb,vc=@vc WHERE i=@i");
PreparedStatement pr = con.prepareStatement( "exec sp_" + tableName + " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" );
long time = -System.currentTimeMillis();
for (int i=0; i<rowCount; i++){
pr.setBytes (  1, byteArray );
pr.setString(  2 , "Test" );
pr.setDate  (  3 , new Date( System.currentTimeMillis() ) );
pr.setFloat (  4, (float)1234.56789 );
pr.setFloat (  5 , (float)9876.54321 );
pr.setBytes (  6, largeByteArray );
pr.setInt   (  7 , i );
pr.setDouble(  8 , 23.45 );
pr.setDouble(  9 , 567.45 );
pr.setFloat (  10 , (float)78.89 );
pr.setTime  (  11, new Time( System.currentTimeMillis() ) );
pr.setShort (  12, (short)23456 );
pr.setFloat (  13, (float)34.56 );
pr.setString(  14, "sysname (30) NULL" );
pr.setString(  15 , "text NULL" );
pr.setByte  (  16, (byte)28 );
pr.setBytes (  17, byteArray );
pr.setString(  18, "varchar (255) NULL" );
int updateCount = pr.executeUpdate();
if (updateCount != 1){
System.out.println( "  Failed: Update count should be 1 but it is " + updateCount + ".");
return;
}
}
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
st.execute("drop procedure sp_"+tableName);
st.close();
pr.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_UpdateRowsPrepareBatch(Connection con){
int batchSize = 10;
int batches = rowCount / batchSize;
System.out.println();
System.out.println( "Test update rows with PreparedStatement and Batches: " + batches + " batches, " + batchSize + " batch size");
try{
PreparedStatement pr = con.prepareStatement( "UPDATE " + tableName + " SET bi=?,c=?,d=?,de=?,f=?,im=?,i=?,m=?,n=?,r=?,sd=?,si=?,sm=?,sy=?,t=?,ti=?,vb=?,vc=? WHERE i=?" );
long time = -System.currentTimeMillis();
for (int i=0; i<batches; i++){
for (int r=0; r<batchSize; r++){
pr.setBytes (  1, byteArray );
pr.setString(  2 , "Test" );
pr.setDate  (  3 , new Date( System.currentTimeMillis() ) );
pr.setFloat (  4, (float)1234.56789 );
pr.setFloat (  5 , (float)9876.54321 );
pr.setBytes (  6, largeByteArray );
pr.setInt   (  7 , i*batchSize + r );
pr.setDouble(  8 , 23.45 );
pr.setDouble(  9 , 567.45 );
pr.setFloat (  10 , (float)78.89 );
pr.setTime  (  11, new Time( System.currentTimeMillis() ) );
pr.setShort (  12, (short)23456 );
pr.setFloat (  13, (float)34.56 );
pr.setString(  14, "sysname (30) NULL" );
pr.setString(  15 , "text NULL" );
pr.setByte  (  16, (byte)28 );
pr.setBytes (  17, byteArray );
pr.setString(  18, "varchar (255) NULL" );
pr.setInt   (  19 , i );
pr.addBatch();
}
int[] updateCount = pr.executeBatch();
if (updateCount.length != batchSize){
System.out.println( "  Failed: Update count size should be " + batchSize + " but it is " + updateCount.length + ".");
return;
}
}
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
pr.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_Scroll_getXXX(Connection con){
System.out.println();
System.out.println( "Test scroll and call the getXXX methods for every columns: " + rowCount + " rows");
try{
Statement st = con.createStatement();
long time = -System.currentTimeMillis();
ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
for (int i=0; i<rowCount; i++){
rs.next();
rs.getInt   (  1 );
rs.getBytes (  2 );
rs.getString(  3 );
rs.getDate  (  4 );
rs.getFloat (  5 );
rs.getFloat (  6 );
rs.getBytes (  7 );
rs.getInt   (  8 );
rs.getDouble(  9 );
rs.getDouble(  10 );
rs.getFloat (  11 );
rs.getTime  (  12 );
rs.getShort (  13 );
rs.getFloat (  14 );
rs.getString(  15 );
rs.getString(  16 );
rs.getByte  (  17 );
rs.getBytes (  18 );
rs.getString(  19 );
}
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
st.close();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_UpdateLargeBinary(Connection con){
System.out.println();
System.out.println( "Test update large binary data: " + rowCount + "KB bytes");
try{
java.io.FileOutputStream fos = new java.io.FileOutputStream(tableName+".bin");
byte bytes[] = new byte[1024];
for(int i=0; i<rowCount; i++){
fos.write(bytes);
}
fos.close();
java.io.FileInputStream fis = new java.io.FileInputStream(tableName+".bin");
long time = -System.currentTimeMillis();
PreparedStatement pr = con.prepareStatement("Update " + tableName + " set im=? WHERE pr=1");
pr.setBinaryStream( 1, fis, rowCount*1024 );
pr.execute();
pr.close();
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
fis.close();
java.io.File file = new java.io.File(tableName+".bin");
file.delete();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void test_UpdateLargeBinaryWithSP(Connection con){
System.out.println();
System.out.println( "Test update large binary data with a SP: " + rowCount + "KB bytes");
try{
java.io.FileOutputStream fos = new java.io.FileOutputStream(tableName+".bin");
byte bytes[] = new byte[1024];
for(int i=0; i<rowCount; i++){
fos.write(bytes);
}
fos.close();
java.io.FileInputStream fis = new java.io.FileInputStream(tableName+".bin");
long time = -System.currentTimeMillis();
Statement st = con.createStatement();
st.execute("CREATE PROCEDURE #UpdateLargeBinary(@im image) as Update " + tableName + " set im=@im WHERE pr=2");
PreparedStatement pr = con.prepareStatement("exec #UpdateLargeBinary ?");
pr.setBinaryStream( 1, fis, rowCount*1024 );
pr.execute();
st.execute("DROP PROCEDURE #UpdateLargeBinary");
st.close();
pr.close();
time += System.currentTimeMillis();
System.out.println( "  Test time: " + time + " ms");
fis.close();
java.io.File file = new java.io.File(tableName+".bin");
file.delete();
}catch(Exception e){
System.out.println("  Failed:"+e);
}finally{
System.out.println();
System.out.println("===================================================================");
}
}
static void createTestTable(Connection con) throws SQLException{
Statement st;
st = con.createStatement();
dropTestTable( con );
st.execute(
"CREATE TABLE " + tableName + " ("+
"    pr  numeric IDENTITY,"+
"    bi  binary (255) NULL ,"+
"    c   nchar (255) NULL ,"+
"    d   datetime NULL ,"+
"    de  decimal(18, 0) NULL ,"+
"    f   float NULL ,"+
"    im  image NULL ,"+
"    i   int NULL ,"+
"    m   money NULL ,"+
"    n   numeric(18, 0) NULL ,"+
"    r   real NULL ,"+
"    sd  smalldatetime NULL ,"+
"    si  smallint NULL ,"+
"    sm  smallmoney NULL ,"+
"    sy  sysname NULL ,"+
"    t   ntext NULL ,"+
"    ti  tinyint NULL ,"+
"    vb  varbinary (255) NULL ,"+
"    vc  nvarchar (255) NULL, "+
"CONSTRAINT PK_BenchTest2 PRIMARY KEY CLUSTERED (pr) "+
")");
st.close();
}
static void deleteTestTable(Connection con){
try{
Statement st = con.createStatement();
st.execute("DELETE FROM " + tableName);
st.close();
}catch(Exception e){
}
static void dropTestTable(Connection con){
try{
Statement st = con.createStatement();
st.execute("drop table " + tableName);
st.close();
}catch(Exception e){
}
static void createTestDataWithClassicInsert(Connection con) throws SQLException{
String sql = "INSERT INTO " + tableName + "(bi,c,d,de,f,im,i,m,n,r,si,sd,sm,sy,t,ti,vb,vc) VALUES(0x172243,'Test','20010101',1234.56789,9876.54321,0x";
for(int i=0; i<largeByteArray.length; i++){
sql += "00";
}
Statement st = con.createStatement();
for (int i=0; i<rowCount; i++){
st.execute(sql + ","+i+",23.45,567.45,78.89,"+i+",'11:11:11',34.56,'sysname (30) NULL','ntext NULL, sample to save in the field',"+(i & 0xFF)+",0x172243,'nvarchar (255) NULL')"  );
}
st.close();
}
}
package smallsql.database;
final class ExpressionFunctionYear extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.YEAR;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
DateTime.Details details = new DateTime.Details(param1.getLong());
return details.year;
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import smallsql.database.language.Language;
public class StoreImpl extends Store {
private static final int DEFAULT_PAGE_SIZE = 8192; 
private static final int PAGE_MAGIC = 0x12DD13DE; 
private static final int PAGE_CONTROL_SIZE = 28;
private static final byte[] page_control = new byte[PAGE_CONTROL_SIZE];
private static final ByteBuffer pageControlBuffer = ByteBuffer.wrap(page_control);
private int status; 
private static final int NORMAL = 0;
private static final int DELETED = 1;
private static final int UPDATE_POINTER = 2;
private static final int UPDATED_PAGE = 3;
final private Table table;
private boolean sharedPageData;
private StorePage storePage;
private long filePos; 
private int sizeUsed;
private int sizePhysical;
private int nextPageOffset;
private long filePosUpdated;
private int type;
private StoreImpl updatePointer;
private StoreImpl( Table table, StorePage storePage, int type, long filePos ){
this.table     = table;
this.storePage    = storePage;
this.filePos   = filePos;
this.type      = type;
}
static StoreImpl createStore( Table table, StorePage storePage, int type, long filePos ) throws SQLException{
try {
StoreImpl store = new StoreImpl(table, storePage, type, filePos);
switch(type){
case SQLTokenizer.LONGVARBINARY:
store.page = new byte[(int)filePos + PAGE_CONTROL_SIZE];
store.filePos = -1;
break;
case SQLTokenizer.INSERT:
case SQLTokenizer.CREATE:
store.page = new byte[DEFAULT_PAGE_SIZE];
break;
case SQLTokenizer.SELECT:
case SQLTokenizer.UPDATE:
case SQLTokenizer.DELETE:
if(storePage.page == null){
FileChannel raFile = storePage.raFile;
synchronized(raFile){
if(filePos >= raFile.size() - PAGE_CONTROL_SIZE){
return null;
}
raFile.position(filePos);
synchronized(page_control){
pageControlBuffer.position(0);
raFile.read(pageControlBuffer);
store.page = page_control;
store.readPageHeader();
}
store.page = new byte[store.sizeUsed];
raFile.position(filePos);
ByteBuffer buffer = ByteBuffer.wrap(store.page);
raFile.read(buffer);
}
}else{
store.page = storePage.page;
store.sharedPageData = true;
store.readPageHeader();
}
store = store.loadUpdatedStore();
break;
default: throw new Error();
}
store.offset = PAGE_CONTROL_SIZE;
return store;
} catch (Throwable th) {
throw SmallSQLException.createFromException(th);
}
}
static StoreImpl recreateStore( Table table, StorePage storePage, int type) throws Exception{
StoreImpl store = new StoreImpl(table, storePage, type, -1);
store.page = storePage.page;
store.sharedPageData = true;
store.readPageHeader();
store = store.loadUpdatedStore();
store.offset = PAGE_CONTROL_SIZE;
return store;
}
private final void readPageHeader() throws SQLException{
if(readInt() != PAGE_MAGIC){
throw SmallSQLException.create(Language.TABLE_CORRUPT_PAGE, new Object[] { new Long(filePos) });
}
status = readInt();
sizeUsed  = readInt();
sizePhysical = readInt();
nextPageOffset = readInt();
filePosUpdated = readLong();
}
final private StoreImpl loadUpdatedStore() throws Exception{
if(status != UPDATE_POINTER) return this;
StoreImpl storeTemp = table.getStore( ((TableStorePage)storePage).con, filePosUpdated, type);
storeTemp.updatePointer = this;
return storeTemp;
}
private void resizePage(int minNewSize){
int newSize = Math.max(minNewSize, page.length*2);
byte[] newPage = new byte[newSize];
System.arraycopy( page, 0, newPage, 0, page.length);
page = newPage;
}
@Override
boolean isValidPage(){
return status == NORMAL || (status == UPDATED_PAGE && updatePointer != null);
}
@Override
int getUsedSize(){
return sizeUsed;
}
@Override
long getNextPagePos(){
if(updatePointer != null) return updatePointer.getNextPagePos();
if(nextPageOffset <= 0){
nextPageOffset = sizePhysical;
}
return filePos + nextPageOffset;
}
long writeFinsh(SSConnection con) throws SQLException{
switch(type){
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.INSERT:
case SQLTokenizer.CREATE:
sizeUsed = sizePhysical = offset;
break;
case SQLTokenizer.UPDATE:
if(status != UPDATE_POINTER) {
sizeUsed = offset;
break;
}
case SQLTokenizer.DELETE:
sizeUsed = PAGE_CONTROL_SIZE;
break;
default: throw new Error(""+type);
}
offset = 0;
writeInt( PAGE_MAGIC ); 
writeInt( status);
writeInt( sizeUsed );
writeInt( sizePhysical );
writeInt( 0 ); 
writeLong( filePosUpdated ); 
storePage.setPageData( page, sizeUsed ); 
if(con == null){
return storePage.commit();
}else{
return 0;
}
}
final void createWriteLock() throws SQLException{
TableStorePage storePageWrite = table.requestWriteLock( ((TableStorePage)storePage).con, (TableStorePage)storePage );
if(storePageWrite == null)
throw SmallSQLException.create(Language.ROW_LOCKED);
storePage = storePageWrite;
}
void updateFinsh(SSConnection con, StoreImpl newData) throws SQLException{
type = SQLTokenizer.UPDATE;
if(newData.offset <= sizePhysical || filePos == -1){
page = newData.page; 
offset = newData.offset;
if(sizePhysical < offset) sizePhysical = offset; 
writeFinsh(con);
}else{
newData.status = UPDATED_PAGE;
if(updatePointer == null){
((TableStorePage)newData.storePage).lockType = TableView.LOCK_INSERT;
filePosUpdated = newData.writeFinsh(null);
status = UPDATE_POINTER;
}else{
((TableStorePage)newData.storePage).lockType = TableView.LOCK_INSERT;
updatePointer.filePosUpdated = newData.writeFinsh(null);
updatePointer.status = UPDATE_POINTER;
updatePointer.type = SQLTokenizer.UPDATE;
updatePointer.createWriteLock();
if(updatePointer.sharedPageData){
updatePointer.page = new byte[PAGE_CONTROL_SIZE];
}
updatePointer.writeFinsh(con);
status = DELETED;
if(sharedPageData){
page = new byte[PAGE_CONTROL_SIZE];
}
}
writeFinsh(con);
}
}
private int offset; 
int getCurrentOffsetInPage(){
return offset;
}
void setCurrentOffsetInPage(int newOffset){
this.offset = newOffset;
}
void writeByte( int value ){
int newSize = offset + 1;
if(newSize > page.length) resizePage(newSize);
page[ offset++ ] = (byte)(value);
}
int readByte(){
return page[ offset++ ];
}
int readUnsignedByte(){
return page[ offset++ ] & 0xFF;
}
void writeBoolean( boolean value ){
int newSize = offset + 1;
if(newSize > page.length) resizePage(newSize);
page[ offset++ ] = (byte)(value ? 1 : 0);
}
boolean readBoolean(){
return page[ offset++ ] != 0;
}
void writeShort( int value ){
int newSize = offset + 2;
if(newSize > page.length) resizePage(newSize);
page[ offset++ ] = (byte)(value >> 8);
page[ offset++ ] = (byte)(value);
}
int readShort(){
return (page[ offset++ ] << 8) | (page[ offset++ ] & 0xFF);
}
void writeInt( int value ){
int newSize = offset + 4;
if(newSize > page.length) resizePage(newSize);
page[ offset++ ] = (byte)(value >> 24);
page[ offset++ ] = (byte)(value >> 16);
page[ offset++ ] = (byte)(value >> 8);
page[ offset++ ] = (byte)(value);
}
int readInt(){
return  ((page[ offset++ ]) << 24) |
((page[ offset++ ] & 0xFF) << 16) |
((page[ offset++ ] & 0xFF) << 8) |
((page[ offset++ ] & 0xFF));
}
void writeLong( long value ){
int newSize = offset + 8;
if(newSize > page.length) resizePage(newSize);
page[ offset++ ] = (byte)(value >> 56);
page[ offset++ ] = (byte)(value >> 48);
page[ offset++ ] = (byte)(value >> 40);
page[ offset++ ] = (byte)(value >> 32);
page[ offset++ ] = (byte)(value >> 24);
page[ offset++ ] = (byte)(value >> 16);
page[ offset++ ] = (byte)(value >> 8);
page[ offset++ ] = (byte)(value);
}
long readLong(){
return  ((long)(page[ offset++ ]) << 56) |
((long)(page[ offset++ ] & 0xFF) << 48) |
((long)(page[ offset++ ] & 0xFF) << 40) |
((long)(page[ offset++ ] & 0xFF) << 32) |
((long)(page[ offset++ ] & 0xFF) << 24) |
((page[ offset++ ] & 0xFF) << 16) |
((page[ offset++ ] & 0xFF) << 8) |
((page[ offset++ ] & 0xFF));
}
void writeDouble(double value){
writeLong( Double.doubleToLongBits(value) );
}
double readDouble(){
return Double.longBitsToDouble( readLong() );
}
void writeFloat(float value){
writeInt( Float.floatToIntBits(value) );
}
float readFloat(){
return Float.intBitsToFloat( readInt() );
}
void writeNumeric( MutableNumeric num){
writeByte( num.getInternalValue().length );
writeByte( num.getScale() );
writeByte( num.getSignum() );
for(int i=0; i<num.getInternalValue().length; i++){
writeInt( num.getInternalValue()[i] );
}
}
MutableNumeric readNumeric(){
int[] value = new int[ readByte() ];
int scale   = readByte();
int signum  = readByte();
for(int i=0; i<value.length; i++){
value[i] = readInt();
}
return new MutableNumeric( signum, value, scale );
}
void writeTimestamp( long ts){
writeLong( ts );
}
long readTimestamp(){
return readLong();
}
void writeTime( long time){
writeInt( (int)((time / 1000) % 86400) );
}
long readTime(){
return readInt() * 1000L;
}
void writeDate( long date){
writeInt( (int)(date / 86400000));
}
long readDate(){
return readInt() * 86400000L;
}
void writeSmallDateTime( long datetime){
writeInt( (int)(datetime / 60000));
}
long readSmallDateTime(){
return readInt() * 60000L;
}
void writeString( String strDaten ) throws SQLException{
writeString( strDaten, Short.MAX_VALUE, true );
}
void writeString( String strDaten, int lengthColumn, boolean varchar ) throws SQLException{
char[] daten = strDaten.toCharArray();
int length = daten.length;
if(lengthColumn < length){
throw SmallSQLException.create(Language.VALUE_STR_TOOLARGE);
}
if(varchar) lengthColumn = length;
int newSize = offset + 2 + 2*lengthColumn;
if(newSize > page.length) resizePage(newSize);
writeShort( lengthColumn );
writeChars( daten );
for(int i=length; i<lengthColumn; i++){
page[ offset++ ] = ' ';
page[ offset++ ] = 0;
}
}
String readString(){
int length = readShort() & 0xFFFF;
return new String( readChars(length) );
}
void writeBytes(byte[] daten){
int newSize = offset + daten.length;
if(newSize > page.length) resizePage(newSize );
System.arraycopy( daten, 0, page, offset, daten.length);
offset += daten.length;
}
void writeBytes(byte[] daten, int off, int length){
int newSize = offset + length;
if(newSize > page.length) resizePage(newSize );
System.arraycopy( daten, off, page, offset, length);
offset += length;
}
byte[] readBytes(int length){
byte[] daten = new byte[length];
System.arraycopy( page, offset, daten, 0, length);
offset += length;
return daten;
}
void writeBinary( byte[] daten, int lengthColumn, boolean varBinary ) throws SQLException{
int length = daten.length;
if(lengthColumn < length){
Object params = new Object[] { new Integer(length), new Integer(lengthColumn) };
throw SmallSQLException.create(Language.VALUE_BIN_TOOLARGE, params);
}
if(varBinary) lengthColumn = length;
int newSize = offset + 2 + lengthColumn;
if(newSize > page.length) resizePage(newSize);
page[ offset++ ] = (byte)(lengthColumn >> 8);
page[ offset++ ] = (byte)(lengthColumn);
writeBytes( daten );
if(!varBinary){
for(int i=length; i<lengthColumn; i++){
page[ offset++ ] = 0;
}
}
}
byte[] readBinary(){
int length = readShort() & 0xFFFF;
return readBytes(length);
}
void writeLongBinary( byte[] daten ) throws Exception{
StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, daten.length + 4, SQLTokenizer.LONGVARBINARY);
store.writeInt( daten.length );
store.writeBytes( daten );
writeLong( store.writeFinsh(null) );
}
byte[] readLongBinary() throws Exception{
long lobFilePos = readLong();
StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, lobFilePos, SQLTokenizer.SELECT );
return store.readBytes( store.readInt() );
}
void writeChars(char[] daten){
int length = daten.length;
int newSize = offset + 2*length;
if(newSize > page.length) resizePage(newSize );
for(int i=0; i<length; i++){
char c = daten[i];
page[ offset++ ] = (byte)(c);
page[ offset++ ] = (byte)(c >> 8);
}
}
char[] readChars(int length){
char[] daten = new char[length];
for(int i=0; i<length; i++){
daten[i] = (char)((page[ offset++ ] & 0xFF) | (page[ offset++ ] << 8));
}
return daten;
}
void writeLongString(String daten) throws Exception{
char[] chars = daten.toCharArray();
StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, chars.length * 2L + 4, SQLTokenizer.LONGVARBINARY);
store.writeInt( chars.length );
store.writeChars( chars );
writeLong( store.writeFinsh(null) );
}
String readLongString() throws Exception{
long lobFilePos = readLong();
StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, lobFilePos, SQLTokenizer.SELECT );
if(store == null) throw SmallSQLException.create(Language.LOB_DELETED);
return new String(store.readChars( store.readInt() ) );
}
void writeColumn(Column column ) throws Exception{
int newSize = offset + 25;
if(newSize > page.length) resizePage(newSize);
writeByte   ( column.getFlag() );
writeString ( column.getName() );
writeShort  ( column.getDataType() );
writeInt    ( column.getPrecision() );
writeByte   ( column.getScale() );
offset += column.initAutoIncrement(storePage.raFile, filePos+offset);
String def = column.getDefaultDefinition();
writeBoolean( def == null );
if(def != null)
writeString ( column.getDefaultDefinition() );
}
Column readColumn(int tableFormatVersion) throws Exception{
Column column = new Column();
column.setFlag( readByte() );
column.setName( readString() );
column.setDataType( readShort() );
int precision;
if(tableFormatVersion == TableView.TABLE_VIEW_OLD_VERSION)
precision = readByte();
else
precision = readInt();
column.setPrecision( precision );
column.setScale( readByte() );
offset += column.initAutoIncrement(storePage.raFile, filePos+offset);
if(!readBoolean()){
String def = readString();
column.setDefaultValue( new SQLParser().parseExpression(def), def);
}
return column;
}
void copyValueFrom( StoreImpl store, int valueOffset, int length){
System.arraycopy( store.page, valueOffset, this.page, this.offset, length);
this.offset += length;
}
void writeExpression( Expression expr, Column column) throws Exception{
boolean isNull = expr.isNull();
if(isNull && !column.isNullable()){
throw SmallSQLException.create(Language.VALUE_NULL_INVALID, column.getName());
}
int dataType = column.getDataType();
if(isNull){
writeBoolean(true); 
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
case SQLTokenizer.TINYINT:
offset++;
break;
case SQLTokenizer.SMALLINT:
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
offset += 2;
break;
case SQLTokenizer.INT:
case SQLTokenizer.REAL:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
offset += 4;
break;
case SQLTokenizer.BIGINT:
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.JAVA_OBJECT:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.TIMESTAMP:
offset += 8;
break;
case SQLTokenizer.UNIQUEIDENTIFIER:
offset += 16;
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
offset += 3;
break;
default: throw new Error();
}
return;
}
writeBoolean(false); 
column.setNewAutoIncrementValue(expr);
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
writeBoolean( expr.getBoolean() );
break;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
writeBinary( expr.getBytes(), column.getPrecision(), dataType != SQLTokenizer.BINARY );
break;
case SQLTokenizer.TINYINT:
writeByte( expr.getInt() );
break;
case SQLTokenizer.SMALLINT:
writeShort( expr.getInt() );
break;
case SQLTokenizer.INT:
writeInt( expr.getInt() );
break;
case SQLTokenizer.BIGINT:
writeLong( expr.getLong() );
break;
case SQLTokenizer.REAL:
writeFloat( expr.getFloat() );
break;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
writeDouble( expr.getDouble() );
break;
case SQLTokenizer.MONEY:
writeLong( expr.getMoney() );
break;
case SQLTokenizer.SMALLMONEY:
writeInt( (int)expr.getMoney() );
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
MutableNumeric numeric = expr.getNumeric();
numeric.setScale( column.getScale() );
writeNumeric( numeric );
break;
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
writeString( expr.getString(), column.getDisplaySize(), false );
break;
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
writeString( expr.getString(), column.getDisplaySize(), true );
break;
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
writeLongString( expr.getString() );
break;
case SQLTokenizer.JAVA_OBJECT:
ByteArrayOutputStream baos = new ByteArrayOutputStream();
ObjectOutputStream oos = new ObjectOutputStream(baos);
oos.writeObject( expr.getObject() );
writeLongBinary( baos.toByteArray() );
break;
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
writeLongBinary( expr.getBytes() );
break;
case SQLTokenizer.TIMESTAMP:
writeTimestamp( expr.getLong() );
break;
case SQLTokenizer.TIME:
writeTime( expr.getLong() );
break;
case SQLTokenizer.DATE:
writeDate( expr.getLong() );
break;
case SQLTokenizer.SMALLDATETIME:
writeSmallDateTime( expr.getLong() );
break;
case SQLTokenizer.UNIQUEIDENTIFIER:
switch(expr.getDataType()){
case SQLTokenizer.UNIQUEIDENTIFIER:
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
byte[] bytes = expr.getBytes();
if(bytes.length != 16) throw SmallSQLException.create(Language.BYTEARR_INVALID_SIZE, String.valueOf(bytes.length));
writeBytes( bytes );
default:
writeBytes( Utils.unique2bytes(expr.getString()) );
}
break;
default: throw new Error(String.valueOf(column.getDataType()));
}
}
@Override
boolean isNull(int valueOffset){
return page[ valueOffset ] != 0;
}
@Override
boolean getBoolean(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return false;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean();
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return Utils.bytes2int( readBinary() ) != 0;
case SQLTokenizer.TINYINT:
return readUnsignedByte() != 0;
case SQLTokenizer.SMALLINT:
return readShort() != 0;
case SQLTokenizer.INT:
return readInt() != 0;
case SQLTokenizer.BIGINT:
return readLong() != 0;
case SQLTokenizer.REAL:
return readFloat() != 0;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return readDouble() != 0;
case SQLTokenizer.MONEY:
return readLong() != 0;
case SQLTokenizer.SMALLMONEY:
return readInt() != 0;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().getSignum() != 0;
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Utils.string2boolean( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Utils.string2boolean( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return Utils.string2boolean( ois.readObject().toString() );
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return Utils.bytes2int( readLongBinary() ) != 0;
case SQLTokenizer.TIMESTAMP:
return readTimestamp() != 0;
case SQLTokenizer.TIME:
return readTime() != 0;
case SQLTokenizer.DATE:
return readDate() != 0;
case SQLTokenizer.SMALLDATETIME:
return readSmallDateTime() != 0;
case SQLTokenizer.UNIQUEIDENTIFIER:
return false;
default:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "BOOLEAN" });
}
}
@Override
int getInt(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? 1 : 0;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return Utils.bytes2int( readBinary() );
case SQLTokenizer.TINYINT:
return readUnsignedByte();
case SQLTokenizer.SMALLINT:
return readShort();
case SQLTokenizer.INT:
return readInt();
case SQLTokenizer.BIGINT:
return (int)readLong();
case SQLTokenizer.REAL:
return (int)readFloat();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return (int)readDouble();
case SQLTokenizer.MONEY:
long longValue = readLong() / 10000;
return Utils.money2int(longValue);
case SQLTokenizer.SMALLMONEY:
return readInt() / 10000;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().intValue();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Integer.parseInt( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Integer.parseInt( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return ExpressionValue.getInt(ois.readObject().toString(), SQLTokenizer.VARCHAR);
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return Utils.bytes2int( readLongBinary() );
case SQLTokenizer.TIMESTAMP:
return (int)readTimestamp();
case SQLTokenizer.TIME:
return (int)readTime();
case SQLTokenizer.DATE:
return (int)readDate();
case SQLTokenizer.SMALLDATETIME:
return (int)readSmallDateTime();
default:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "INT" });
}
}
@Override
long getLong(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? 1 : 0;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return Utils.bytes2long( readBinary() );
case SQLTokenizer.TINYINT:
return readUnsignedByte();
case SQLTokenizer.SMALLINT:
return readShort();
case SQLTokenizer.INT:
return readInt();
case SQLTokenizer.BIGINT:
return readLong();
case SQLTokenizer.REAL:
return (long)readFloat();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return (long)readDouble();
case SQLTokenizer.MONEY:
return readLong() / 10000;
case SQLTokenizer.SMALLMONEY:
return readInt() / 10000;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().longValue();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Long.parseLong( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Long.parseLong( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return ExpressionValue.getLong( ois.readObject().toString(), SQLTokenizer.VARCHAR );
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return Utils.bytes2long( readLongBinary() );
case SQLTokenizer.TIMESTAMP:
return readTimestamp();
case SQLTokenizer.TIME:
return readTime();
case SQLTokenizer.DATE:
return readDate();
case SQLTokenizer.SMALLDATETIME:
return readSmallDateTime();
default:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "BIGINT" });
}
}
@Override
float getFloat(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? 1 : 0;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return Utils.bytes2float( readBinary() );
case SQLTokenizer.TINYINT:
return readUnsignedByte();
case SQLTokenizer.SMALLINT:
return readShort();
case SQLTokenizer.INT:
return readInt();
case SQLTokenizer.BIGINT:
return readLong();
case SQLTokenizer.REAL:
return readFloat();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return (float)readDouble();
case SQLTokenizer.MONEY:
return readLong() / (float)10000.0;
case SQLTokenizer.SMALLMONEY:
return readInt() / (float)10000.0;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().floatValue();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Float.parseFloat( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Float.parseFloat( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return Float.parseFloat( ois.readObject().toString() );
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return Utils.bytes2float( readLongBinary() );
case SQLTokenizer.TIMESTAMP:
return readTimestamp();
case SQLTokenizer.TIME:
return readTime();
case SQLTokenizer.DATE:
return readDate();
case SQLTokenizer.SMALLDATETIME:
return readSmallDateTime();
default:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "REAL" });
}
}
@Override
double getDouble(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? 1 : 0;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return Utils.bytes2double( readBinary() );
case SQLTokenizer.TINYINT:
return readUnsignedByte();
case SQLTokenizer.SMALLINT:
return readShort();
case SQLTokenizer.INT:
return readInt();
case SQLTokenizer.BIGINT:
return readLong();
case SQLTokenizer.REAL:
return readFloat();
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return readDouble();
case SQLTokenizer.MONEY:
return readLong() / 10000.0;
case SQLTokenizer.SMALLMONEY:
return readInt() / 10000.0;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().doubleValue();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Double.parseDouble( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Double.parseDouble( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return Double.parseDouble( ois.readObject().toString() );
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return Utils.bytes2double( readLongBinary() );
case SQLTokenizer.TIMESTAMP:
return readTimestamp();
case SQLTokenizer.TIME:
return readTime();
case SQLTokenizer.DATE:
return readDate();
case SQLTokenizer.SMALLDATETIME:
return readSmallDateTime();
default:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "NUMERIC" });
}
}
@Override
long getMoney( int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? 10000 : 0;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return (long)(Utils.bytes2double( readBinary() ) * 10000L);
case SQLTokenizer.TINYINT:
return readUnsignedByte() * 10000L;
case SQLTokenizer.SMALLINT:
return readShort() * 10000L;
case SQLTokenizer.INT:
return readInt() * 10000L;
case SQLTokenizer.BIGINT:
return readLong() * 10000L;
case SQLTokenizer.REAL:
return (long)(readFloat() * 10000L);
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return (long)(readDouble() * 10000L);
case SQLTokenizer.MONEY:
return readLong();
case SQLTokenizer.SMALLMONEY:
return readInt();
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return (long)(readNumeric().doubleValue() * 10000L);
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Money.parseMoney( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Money.parseMoney( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return Money.parseMoney( ois.readObject().toString() );
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return (long)(Utils.bytes2double( readLongBinary() ) * 10000L);
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "MONEY" });
default: throw new Error();
}
}
@Override
MutableNumeric getNumeric(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return null;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? new MutableNumeric(1) : new MutableNumeric(0);
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return new MutableNumeric(Utils.bytes2double( readBinary() ));
case SQLTokenizer.TINYINT:
return new MutableNumeric(readUnsignedByte());
case SQLTokenizer.SMALLINT:
return new MutableNumeric(readShort());
case SQLTokenizer.INT:
return new MutableNumeric(readInt());
case SQLTokenizer.BIGINT:
return new MutableNumeric(readLong());
case SQLTokenizer.REAL:
return new MutableNumeric(readFloat());
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return new MutableNumeric(readDouble());
case SQLTokenizer.MONEY:
return new MutableNumeric( readLong(), 4);
case SQLTokenizer.SMALLMONEY:
return new MutableNumeric( readInt(), 4);
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return new MutableNumeric( readString() );
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return new MutableNumeric( readLongString() );
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return new MutableNumeric( ois.readObject().toString() );
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return new MutableNumeric( Utils.bytes2double( readLongBinary() ) );
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "NUMERIC" });
default: throw new Error();
}
}
@Override
Object getObject(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return null;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return readBoolean() ? Boolean.TRUE : Boolean.FALSE;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return readBinary();
case SQLTokenizer.TINYINT:
return Utils.getInteger( readUnsignedByte() );
case SQLTokenizer.SMALLINT:
return Utils.getInteger( readShort() );
case SQLTokenizer.INT:
return Utils.getInteger(readInt());
case SQLTokenizer.BIGINT:
return new Long(readLong());
case SQLTokenizer.REAL:
return new Float( readFloat() );
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return new Double( readDouble() );
case SQLTokenizer.MONEY:
return Money.createFromUnscaledValue(readLong());
case SQLTokenizer.SMALLMONEY:
return Money.createFromUnscaledValue(readInt());
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return readString();
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return readLongString();
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return ois.readObject();
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return readLongBinary();
case SQLTokenizer.TIMESTAMP:
return new DateTime( readTimestamp(), SQLTokenizer.TIMESTAMP );
case SQLTokenizer.TIME:
return new DateTime( readTime(), SQLTokenizer.TIME );
case SQLTokenizer.DATE:
return new DateTime( readDate(), SQLTokenizer.DATE );
case SQLTokenizer.SMALLDATETIME:
return new DateTime( readSmallDateTime(), SQLTokenizer.TIMESTAMP );
case SQLTokenizer.UNIQUEIDENTIFIER:
return Utils.bytes2unique( page, this.offset);
default: throw new Error();
}
}
@Override
String getString( int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return null;
switch(dataType){
case SQLTokenizer.BIT:
return readBoolean() ? "1" : "0";
case SQLTokenizer.BOOLEAN:
return String.valueOf( readBoolean() );
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return Utils.bytes2hex( readBinary() );
case SQLTokenizer.TINYINT:
return String.valueOf( readUnsignedByte() );
case SQLTokenizer.SMALLINT:
return String.valueOf( readShort() );
case SQLTokenizer.INT:
return String.valueOf( readInt() );
case SQLTokenizer.BIGINT:
return String.valueOf( readLong() );
case SQLTokenizer.REAL:
return String.valueOf( readFloat() );
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return String.valueOf( readDouble() );
case SQLTokenizer.MONEY:
return Money.createFromUnscaledValue( readLong() ).toString();
case SQLTokenizer.SMALLMONEY:
return Money.createFromUnscaledValue( readInt() ).toString();
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().toString();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return readString();
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return readLongString();
case SQLTokenizer.JAVA_OBJECT:
ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
ObjectInputStream ois = new ObjectInputStream(bais);
return ois.readObject().toString();
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return Utils.bytes2hex( readLongBinary() );
case SQLTokenizer.TIMESTAMP:
return new DateTime( readTimestamp(), SQLTokenizer.TIMESTAMP ).toString();
case SQLTokenizer.TIME:
return new DateTime( readTime(), SQLTokenizer.TIME ).toString();
case SQLTokenizer.DATE:
return new DateTime( readDate(), SQLTokenizer.DATE ).toString();
case SQLTokenizer.SMALLDATETIME:
return new DateTime( readSmallDateTime(), SQLTokenizer.TIMESTAMP ).toString();
case SQLTokenizer.UNIQUEIDENTIFIER:
return Utils.bytes2unique( page, this.offset);
default: throw new Error();
}
}
@Override
byte[] getBytes(int valueOffset, int dataType) throws Exception{
this.offset = valueOffset;
if(readBoolean()) return null;
switch(dataType){
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
return readBinary();
case SQLTokenizer.TINYINT:
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
byte[] bytes = new byte[1];
System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
return bytes;
case SQLTokenizer.SMALLINT:
bytes = new byte[2];
System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
return bytes;
case SQLTokenizer.INT:
case SQLTokenizer.REAL:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
bytes = new byte[4];
System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
return bytes;
case SQLTokenizer.BIGINT:
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.TIMESTAMP:
bytes = new byte[8];
System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
return bytes;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
return readNumeric().toByteArray();
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return readString().getBytes();
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return readLongString().getBytes();
case SQLTokenizer.JAVA_OBJECT:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
return readLongBinary();
case SQLTokenizer.UNIQUEIDENTIFIER:
bytes = new byte[16];
System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
return bytes;
default: throw new Error();
}
}
@Override
void scanObjectOffsets( int[] offsets, int dataTypes[] ){
offset = PAGE_CONTROL_SIZE;
for(int i=0; i<offsets.length; i++){
offsets[i] = offset;
boolean isNull = readBoolean(); 
switch(dataTypes[i]){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
case SQLTokenizer.TINYINT:
offset++;
break;
case SQLTokenizer.SMALLINT:
offset += 2;
break;
case SQLTokenizer.INT:
case SQLTokenizer.REAL:
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
offset += 4;
break;
case SQLTokenizer.BIGINT:
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.JAVA_OBJECT:
case SQLTokenizer.LONGVARBINARY:
case SQLTokenizer.BLOB:
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
case SQLTokenizer.TIMESTAMP:
offset += 8;
break;
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
int count = readShort() & 0xFFFF;
if(!isNull) offset += count;  
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
count = readByte();
offset += 2;
if(!isNull) offset += count*4;
break;
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
count = readShort() & 0xFFFF;
if(!isNull) offset += count << 1; 
break;
case SQLTokenizer.UNIQUEIDENTIFIER:
offset += 16;
break;
default: throw new Error(String.valueOf( dataTypes[i] ) );
}
}
}
@Override
void deleteRow(SSConnection con) throws SQLException{
status = DELETED;
type   = SQLTokenizer.DELETE;
createWriteLock();
writeFinsh(con);
}
StorePageLink getLink(){
return ((TableStorePageInsert)storePage).getLink();
}
boolean isRollback(){
return storePage.raFile == null;
}
}
package smallsql.database;
final class ExpressionFunctionSign extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.SIGN;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
switch(ExpressionArithmetic.getBestNumberDataType(param1.getDataType())){
case SQLTokenizer.INT:
int intValue = param1.getInt();
if(intValue < 0)
return -1;
if(intValue > 0)
return 1;
return 0;
case SQLTokenizer.BIGINT:
long longValue = param1.getLong();
if(longValue < 0)
return -1;
if(longValue > 0)
return 1;
return 0;
case SQLTokenizer.MONEY:
longValue = param1.getMoney();
if(longValue < 0)
return -1;
if(longValue > 0)
return 1;
return 0;
case SQLTokenizer.DECIMAL:
return param1.getNumeric().getSignum();
case SQLTokenizer.DOUBLE:
double doubleValue = param1.getDouble();
if(doubleValue < 0)
return -1;
if(doubleValue > 0)
return 1;
return 0;
default:
throw new Error();
}
}
}
package smallsql.database;
final class ExpressionFunctionDifference extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.DIFFERENCE;
}
boolean isNull() throws Exception {
return param1.isNull() || param2.isNull();
}
final int getInt() throws Exception {
if(isNull()) return 0;
String str1 = ExpressionFunctionSoundex.getString(param1.getString());
String str2 = ExpressionFunctionSoundex.getString(param2.getString());
int diff = 0;
for(int i=0; i<4; i++){
if(str1.charAt(i) == str2.charAt(i)){
diff++;
}
}
return diff;
}
}
package smallsql.database;
final class ExpressionFunctionAscii extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.ASCII;
}
final boolean isNull() throws Exception {
return param1.isNull() || param1.getString().length() == 0;
}
final int getInt() throws Exception {
String str = param1.getString();
if(str == null || str.length() == 0) return 0;
return str.charAt(0);
}
final Object getObject() throws Exception {
String str = param1.getString();
if(str == null || str.length() == 0) return null;
return Utils.getInteger(str.charAt(0));
}
}
package smallsql.database;
import smallsql.database.language.Language;
final class ExpressionFunctionSubstring extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.SUBSTRING;
}
final boolean isNull() throws Exception {
return param1.isNull() || param2.isNull() || param3.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int byteLen = bytes.length;
int start  = Math.min( Math.max( 0, param2.getInt() - 1), byteLen);
int length = param3.getInt();
if(length < 0)
throw SmallSQLException.create(Language.SUBSTR_INVALID_LEN, new Integer(length));
if(start == 0 && byteLen == length) return bytes;
if(byteLen > length + start){
byte[] b = new byte[length];
System.arraycopy(bytes, start, b, 0, length);
return b;
}else{
byte[] b = new byte[byteLen - start];
System.arraycopy(bytes, start, b, 0, b.length);
return b;
}
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int strLen = str.length();
int start  = Math.min( Math.max( 0, param2.getInt() - 1), strLen);
int length = param3.getInt();
if(length < 0)
throw SmallSQLException.create(Language.SUBSTR_INVALID_LEN, new Integer(length));
length = Math.min( length, strLen-start );
return str.substring(start, start+length);
}
}
package smallsql.database;
class StorePageMap {
private Entry[] table;
private int size;
private int threshold;
StorePageMap() {
threshold = 12;
table = new Entry[17];
}
final int size() {
return size;
}
final boolean isEmpty() {
return size == 0;
}
final TableStorePage get(long key) {
int i = (int)(key % table.length);
Entry e = table[i];
while (true) {
if (e == null)
return null;
if (e.key == key)
return e.value;
e = e.next;
}
}
final boolean containsKey(long key) {
return (get(key) != null);
}
final TableStorePage add(long key, TableStorePage value) {
int i = (int)(key % table.length);
table[i] = new Entry(key, value, table[i]);
if (size++ >= threshold)
resize(2 * table.length);
return null;
}
final private void resize(int newCapacity) {
Entry[] newTable = new Entry[newCapacity];
transfer(newTable);
table = newTable;
threshold = (int)(newCapacity * 0.75f);
}
final private void transfer(Entry[] newTable) {
Entry[] src = table;
int newCapacity = newTable.length;
for (int j = 0; j < src.length; j++) {
Entry e = src[j];
if (e != null) {
src[j] = null;
do {
Entry next = e.next;
e.next = null;
int i = (int)(e.key % newCapacity);
if(newTable[i] == null){
newTable[i] = e;
}else{
Entry entry = newTable[i];
while(entry.next != null) entry = entry.next;
entry.next = e;
}
e = next;
} while (e != null);
}
}
}
final TableStorePage remove(long key) {
int i = (int)(key % table.length);
Entry prev = table[i];
Entry e = prev;
while (e != null) {
Entry next = e.next;
if (e.key == key) {
size--;
if (prev == e)
table[i] = next;
else
prev.next = next;
return e.value;
}
prev = e;
e = next;
}
return null;
}
final void clear() {
Entry tab[] = table;
for (int i = 0; i < tab.length; i++)
tab[i] = null;
size = 0;
}
final boolean containsValue(TableStorePage value) {
Entry tab[] = table;
for (int i = 0; i < tab.length ; i++)
for (Entry e = tab[i] ; e != null ; e = e.next)
if (value.equals(e.value))
return true;
return false;
}
static class Entry{
final long key;
final TableStorePage value;
Entry next;
Entry(long k, TableStorePage v, Entry n) {
value = v;
next = n;
key = k;
}
}
}
package smallsql.database;
import java.math.*;
public class Money extends Number implements Mutable{
private static final long serialVersionUID = -620300937494609089L;
long value;
private Money(){
public Money(double value){
this.value = (long)(value * 10000);
}
public Money(float value){
this.value = (long)(value * 10000);
}
public static Money createFromUnscaledValue(long value){
Money money = new Money();
money.value = value;
return money;
}
public static Money createFromUnscaledValue(int value){
Money money = new Money();
money.value = value;
return money;
}
public int intValue() {
return (int)(value / 10000.0);
}
public float floatValue() {
return value / 10000.0F;
}
public double doubleValue() {
return value / 10000.0;
}
public long longValue() {
return (long)(value / 10000.0);
}
public String toString(){
StringBuffer buffer = new StringBuffer();
buffer.append(longValue()).append('.');
final long v = Math.abs(value);
buffer.append( (char)((v % 10000) / 1000 + '0') );
buffer.append( (char)((v % 1000) / 100 + '0') );
buffer.append( (char)((v % 100) / 10 + '0') );
buffer.append( (char)((v % 10) + '0') );
return buffer.toString();
}
public boolean equals(Object obj){
return (obj instanceof Money && ((Money)obj).value == value);
}
public int hashCode(){
return (int)(value ^ (value >>> 32));
}
public long unscaledValue(){
return value;
}
public static long parseMoney( String str ){
return Utils.doubleToMoney(Double.parseDouble( str ));
}
private byte[] toByteArray(){
byte[] bytes = new byte[8];
int offset = 0;
bytes[offset++] = (byte)(value >> 56);
bytes[offset++] = (byte)(value >> 48);
bytes[offset++] = (byte)(value >> 40);
bytes[offset++] = (byte)(value >> 32);
bytes[offset++] = (byte)(value >> 24);
bytes[offset++] = (byte)(value >> 16);
bytes[offset++] = (byte)(value >> 8);
bytes[offset++] = (byte)(value);
return bytes;
}
public BigDecimal toBigDecimal(){
if(value == 0) return ZERO;
return new BigDecimal( new BigInteger( toByteArray() ), 4 );
}
public Object getImmutableObject(){
return toBigDecimal();
}
static private final BigDecimal ZERO = new BigDecimal("0.0000");
}
package smallsql.database;
final class ExpressionFunctionDayOfYear extends ExpressionFunctionReturnInt {
final int getFunction() {
return SQLTokenizer.DAYOFYEAR;
}
final int getInt() throws Exception {
if(param1.isNull()) return 0;
DateTime.Details details = new DateTime.Details(param1.getLong());
return details.dayofyear+1;
}
}
package smallsql.junit;
import java.sql.*;
public class TestAlterTable extends BasicTestCase {
private final String table = "AlterTable";
private final int rowCount = 10;
public void setUp(){
tearDown();
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("create table " + table + "(i int, v varchar(100))");
st.execute("Insert into " + table + " Values(1,'abc')");
st.execute("Insert into " + table + " Values(2,'bcd')");
st.execute("Insert into " + table + " Values(3,'cde')");
st.execute("Insert into " + table + " Values(4,'def')");
st.execute("Insert into " + table + " Values(5,'efg')");
st.execute("Insert into " + table + " Values(6,'fgh')");
st.execute("Insert into " + table + " Values(7,'ghi')");
st.execute("Insert into " + table + " Values(8,'hij')");
st.execute("Insert into " + table + " Values(9,'ijk')");
st.execute("Insert into " + table + " Values(10,'jkl')");
st.close();
}catch(Throwable e){
e.printStackTrace();
}
}
public void tearDown(){
try {
dropTable( AllTests.getConnection(), table );
} catch (SQLException ex) {
ex.printStackTrace();
}
}
public void testAdd1Column() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("Alter Table " + table + " Add a Varchar(20)");
ResultSet rs = st.executeQuery("Select * From " + table);
assertRSMetaData( rs, new String[]{"i", "v", "a"},  new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR} );
}
public void testAdd2Column() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("Alter Table " + table + " Add a Varchar(20), b int DEFAULT 25");
ResultSet rs = st.executeQuery("Select * From " + table);
assertRSMetaData( rs, new String[]{"i", "v", "a", "b"},  new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER} );
int count = 0;
while(rs.next()){
assertEquals( "default value", 25, rs.getInt("b") );
count++;
}
assertEquals( "RowCount", rowCount, count );
}
public void testAddWithTableLock_REPEATABLE_READ() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
int isolation = con.getTransactionIsolation();
con.setAutoCommit(false);
try{
con.setTransactionIsolation( Connection.TRANSACTION_REPEATABLE_READ );
ResultSet rs = st.executeQuery("Select * From " + table);
rs.next();
try {
st.execute("Alter Table " + table + " Add a Varchar(20)");
fail("Alter Table should not work on a table with a lock.");
} catch (SQLException ex) {
assertSQLException( "01000", 0, ex );
}
rs.next();
}finally{
con.setTransactionIsolation(isolation);
con.setAutoCommit(true);
}
}
public void testAddWithTableLock_READ_COMMITTED() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
int isolation = con.getTransactionIsolation();
con.setAutoCommit(false);
try{
con.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
ResultSet rs = st.executeQuery("Select * From " + table);
rs.next();
st.execute("Alter Table " + table + " Add a Varchar(20)");
try {
rs.next();
fail("Alter Table should not work on a table with a lock.");
} catch (SQLException ex) {
assertSQLException( "01000", 0, ex );
}
}finally{
con.setTransactionIsolation(isolation);
con.setAutoCommit(true);
}
}
}
package smallsql.database;
import java.sql.*;
class ForeignKey {
final String pkTable;
final String fkTable;
final IndexDescription pk;
final IndexDescription fk;
final int updateRule = DatabaseMetaData.importedKeyNoAction;
final int deleteRule = DatabaseMetaData.importedKeyNoAction;
ForeignKey(String pkTable, IndexDescription pk, String fkTable, IndexDescription fk){
this.pkTable = pkTable;
this.fkTable = fkTable;
this.pk = pk;
this.fk = fk;
}
}
package smallsql.database;
class SQLToken{
int value;
int offset;  
int length;  
String name;
SQLToken (int value, int tokenStart, int tokenEnd){
this.value  = value;
this.offset = tokenStart;
this.length = tokenEnd-tokenStart;
}
SQLToken (String name, int value, int tokenStart, int tokenEnd){
this.value  = value;
this.offset = tokenStart;
this.length = tokenEnd-tokenStart;
this.name   = name;
}
String getName(char[] sql){
if(name != null) return name;
return new String( sql, offset, length );
}
}
package smallsql.database;
import java.io.ByteArrayOutputStream;
public class ExpressionFunctionRepeat extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.REPEAT;
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int count  = param2.getInt();
ByteArrayOutputStream buffer = new ByteArrayOutputStream();
for(int i=0; i<count; i++){
buffer.write(bytes);
}
return buffer.toByteArray();
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int count  = param2.getInt();
StringBuffer buffer = new StringBuffer();
for(int i=0; i<count; i++){
buffer.append(str);
}
return buffer.toString();
}
int getPrecision() {
return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
}
}
package smallsql.junit;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import smallsql.database.language.Language;
public class TestLanguage extends BasicTestCase {
private static final String TABLE_NAME = "test_lang";
private static final String[] OTHER_LANGUAGES = { "it", "de" };
public void setUp() throws SQLException {
tearDown();
}
public void tearDown() throws SQLException {
Connection conn = AllTests.createConnection("?locale=en", null);
try {
conn.prepareStatement("DROP TABLE " + TABLE_NAME).execute();
}
catch (Exception e) {}
finally {
conn.close();
}
}
public void testBogusLocale() throws SQLException {
Locale origLocale = Locale.getDefault();
Locale.setDefault(Locale.ITALY);
Connection conn = AllTests.createConnection("?locale=XXX", null);
Statement stat = conn.createStatement();
try {
recreateTestTab(stat);
stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
fail();
}
catch (SQLException e) {
assertMessage(e, "La tabella/vista '" + TABLE_NAME + "' è già esistente.");
}
finally {
Locale.setDefault(origLocale);
conn.close();
}
}
public void testLocalizedErrors() throws Exception {
Connection conn = AllTests.createConnection("?locale=it", null);
Statement stat = conn.createStatement();
try {
try {
recreateTestTab(stat);
stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
fail();
}
catch(SQLException e) {
assertMessage(e, "La tabella/vista '" + TABLE_NAME + "' è già esistente.");
}
try {
stat.execute("DROP TABLE " + TABLE_NAME);
stat.execute("DROP TABLE " + TABLE_NAME);
}
catch (SQLException e) {
assertMessage(e, "Non si può effettuare DROP della tabella");
}
try {
stat.execute("CREATE TABLE foo");
}
catch (SQLException e) {
assertMessage(e, "Errore di sintassi, fine inattesa");
}
}
finally {
conn.close();
}
}
public void testSyntaxErrors() throws SQLException {
Connection conn = AllTests.createConnection("?locale=it", null);
Statement stat = conn.createStatement();
try {
try {
stat.execute("CREATE TABLE");
}
catch (SQLException se) {
assertMessage(se, "Errore di sintassi, fine inattesa della stringa SQL. Le parole chiave richieste sono: <identifier>");
}
try {
stat.execute("Some nonsensical sentence.");
}
catch (SQLException se) {
assertMessage(se, "Errore di sintassi alla posizione 0 in 'Some'. Le parole chiave richieste sono");
}
recreateTestTab(stat);
try {
stat.execute("SELECT bar() FROM foo");
}
catch (SQLException se) {
assertMessage(se, "Errore di sintassi alla posizione 7 in 'bar'. Funzione sconosciuta");
}
try {
stat.execute("SELECT UCASE('a', '');");
}
catch (SQLException se) {
assertMessage(se, "Errore di sintassi alla posizione 7 in 'UCASE'. Totale parametri non valido.");
}
}
finally {
conn.close();
}
}
private void assertMessage(SQLException e, String expectedText) {
assertMessage(e, new String[] { expectedText });
}
private void assertMessage(SQLException e, String[] expectedTexts) {
String message = e.getMessage();
boolean found = true;
for (int i = 0; found && i < expectedTexts.length; i++) {
found = found && message.indexOf(expectedTexts[i]) >= 0;
}
if (! found) {
System.err.println("ERROR [Wrong message]:" + message);
fail();
}
}
private void recreateTestTab(Statement stat) throws SQLException {
stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
}
public void testEntries() throws Exception {
boolean failed = false;
StringBuffer msgBuf = new StringBuffer();
Language eng = Language.getLanguage("en");
HashSet engEntriesSet = new HashSet();
String[][] engEntriesArr = eng.getEntries();
Set diff = (Set)engEntriesSet.clone();
diff.removeAll(otherEntriesSet);
if (diff.size() > 0) {
failed = true;
msgBuf.append("\nMissing entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
for (Iterator itr = diff.iterator(); itr.hasNext(); ) {
msgBuf.append(itr.next());
if (itr.hasNext()) msgBuf.append(',');
}
}
StringBuffer buf = new StringBuffer();
for (int j = 1; j < engEntriesArr.length; j++) {
String key = engEntriesArr[j][0];
String engValue = eng.getMessage(key);
String otherValue = lang2.getMessage(key);
if(engValue.equals(otherValue)){
failed = true;
if(buf.length() > 0){
buf.append(',');
}
buf.append(key);
}
}
if(buf.length()>0){
msgBuf.append("\nNot translated entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
msgBuf.append(buf);
}
}
if (failed){
System.err.println(msgBuf);
fail(msgBuf.toString());
}
}
}
package smallsql.database;
abstract class ExpressionFunctionReturnP1Number extends ExpressionFunctionReturnP1 {
final boolean getBoolean() throws Exception{
return getDouble() != 0;
}
final int getInt() throws Exception {
return Utils.long2int(getLong());
}
final long getLong() throws Exception{
return Utils.double2long(getDouble());
}
final float getFloat() throws Exception {
return (float)getDouble();
}
MutableNumeric getNumeric() throws Exception{
if(param1.isNull()) return null;
switch(getDataType()){
case SQLTokenizer.INT:
return new MutableNumeric(getInt());
case SQLTokenizer.BIGINT:
return new MutableNumeric(getLong());
case SQLTokenizer.MONEY:
return new MutableNumeric(getMoney(), 4);
case SQLTokenizer.DECIMAL:
MutableNumeric num = param1.getNumeric();
num.floor();
return num;
case SQLTokenizer.DOUBLE:
return new MutableNumeric(getDouble());
default:
throw new Error();
}
}
long getMoney() throws Exception{
return Utils.doubleToMoney(getDouble());
}
String getString() throws Exception {
if(isNull()) return null;
return getObject().toString();
}
final int getDataType() {
return ExpressionArithmetic.getBestNumberDataType(param1.getDataType());
}
}
package smallsql.database;
final class Join extends RowSource{
Expression condition; 
private int type;
RowSource left; 
RowSource right;
private boolean isAfterLast;
private LongLongList rowPositions; 
private int row; 
JoinScroll scroll;
Join( int type, RowSource left, RowSource right, Expression condition ){
this.type = type;
this.condition = condition;
this.left = left;
this.right = right;
}
final boolean isScrollable(){
return false; 
}
void beforeFirst() throws Exception{
scroll.beforeFirst();
isAfterLast  = false;
row = 0;
}
boolean first() throws Exception{
beforeFirst();
return next();
}
boolean next() throws Exception{
if(isAfterLast) return false;
row++;
boolean result = scroll.next();
if(!result){
noRow();
}
return result;
}
void afterLast(){
isAfterLast = true;
noRow();
}
int getRow(){
return row;
}
final long getRowPosition(){
if(rowPositions == null) rowPositions = new LongLongList();
rowPositions.add( left.getRowPosition(), right.getRowPosition());
return rowPositions.size()-1;
}
final void setRowPosition(long rowPosition) throws Exception{
left .setRowPosition( rowPositions.get1((int)rowPosition));
right.setRowPosition( rowPositions.get2((int)rowPosition));
}
final boolean rowInserted(){
return left.rowInserted() || right.rowInserted();
}
final boolean rowDeleted(){
return left.rowDeleted() || right.rowDeleted();
}
void nullRow(){
left.nullRow();
right.nullRow();
row = 0;
}
void noRow(){
isAfterLast = true;
left.noRow();
right.noRow();
row = 0;
}
void execute() throws Exception{
left.execute();
right.execute();
if(!createJoinScrollIndex()){
scroll = new JoinScroll(type, left, right, condition);
}
}
boolean isExpressionsFromThisRowSource(Expressions columns){
if(left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)){
return true;
}
if(columns.size() == 1){
return false;
}
Expressions single = new Expressions();
for(int i=0; i<columns.size(); i++){
single.clear();
single.add(columns.get(i));
if(left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)){
continue;
}
return false;
}
return true;
}
private boolean createJoinScrollIndex() throws Exception{
if(type == CROSS_JOIN){
return false;
}
if(type != INNER_JOIN){
return false;
}
if(condition instanceof ExpressionArithmetic){
ExpressionArithmetic cond = (ExpressionArithmetic)condition;
Expressions leftEx = new Expressions();
Expressions rightEx = new Expressions();
int operation = createJoinScrollIndex(cond, leftEx, rightEx, 0);
if(operation != 0){
scroll = new JoinScrollIndex( type, left, right, leftEx, rightEx, operation);
return true;
}
}
return false;
}
private int createJoinScrollIndex(ExpressionArithmetic cond, Expressions leftEx, Expressions rightEx, int operation) throws Exception{
Expression[] params = cond.getParams();
int op = cond.getOperation();
if(op == ExpressionArithmetic.AND){
Expression param0 = params[0];
Expression param1 = params[1];
if(param0 instanceof ExpressionArithmetic && param1 instanceof ExpressionArithmetic){
op = createJoinScrollIndex((ExpressionArithmetic)param0, leftEx, rightEx, operation);
if(op == 0){
return 0;
}
return createJoinScrollIndex((ExpressionArithmetic)param1, leftEx, rightEx, operation);
}
return 0;
}
if(operation == 0){
operation = op;
}
if(operation != op){
return 0;
}
if(operation == ExpressionArithmetic.EQUALS){
Expression param0 = params[0];
Expression param1 = params[1];
Expressions columns0 = Utils.getExpressionNameFromTree(param0);
Expressions columns1 = Utils.getExpressionNameFromTree(param1);
if(left.isExpressionsFromThisRowSource(columns0) && right.isExpressionsFromThisRowSource(columns1)){
leftEx.add( param0 );
rightEx.add( param1 );
}else{
if(left.isExpressionsFromThisRowSource(columns1) && right.isExpressionsFromThisRowSource(columns0)){
leftEx.add( param1 );
rightEx.add( param0 );
}else{
return 0;
}
}
return operation;
}
return 0;
}
static final int CROSS_JOIN = 1;
static final int INNER_JOIN = 2;
static final int LEFT_JOIN  = 3;
static final int FULL_JOIN  = 4;
static final int RIGHT_JOIN = 5;
}
package smallsql.database;
public class ExpressionFunctionLTrim extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.LTRIM;
}
final boolean isNull() throws Exception {
return param1.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int start = 0;
int length = bytes.length;
while(start<length && bytes[start]==0){
start++;
}
length -= start;
byte[] b = new byte[length];
System.arraycopy(bytes, start, b, 0, length);
return b;
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int start = 0;
while(start<str.length() && str.charAt(start)==' '){
start++;
}
return str.substring(start);
}
}
package smallsql.database;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.DriverManager;
import java.sql.SQLException;
import smallsql.database.language.Language;
final class IndexDescription {
static final int MAGIC_INDEX = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'I';
static final int INDEX_VERSION = 1;
private final String name;
final private int constraintType; 
final private Strings columns;
private int[] matrix;
final private Expressions expressions;
private Index index;
private FileChannel raFile;
IndexDescription( String name, String tableName, int constraintType, Expressions expressions, Strings columns){
this.constraintType = constraintType;
this.expressions = expressions;
this.columns = columns;
this.name = createName(name, tableName);
}
private static String createName( String defaultName, String tableName ){
if(defaultName == null){
defaultName = tableName + "_" + Long.toHexString(System.currentTimeMillis()) + Integer.toHexString(new Object().hashCode());
}
return defaultName;
}
final String getName(){
return name;
}
final boolean isPrimary(){
return constraintType == SQLTokenizer.PRIMARY;
}
final boolean isUnique(){
return constraintType == SQLTokenizer.PRIMARY || constraintType == SQLTokenizer.UNIQUE;
}
final Strings getColumns(){
return columns;
}
final int matchFactor(Strings strings){
if(strings.size() < columns.size())
return Integer.MAX_VALUE; 
nextColumn:
for(int c=0; c<columns.size(); c++){
String colName = columns.get(c);
for(int s=0; s<strings.size(); s++){
if(colName.equalsIgnoreCase(strings.get(s)) )
continue nextColumn;
}
return Integer.MAX_VALUE; 
}
return strings.size() - columns.size();
}
final void init(Database database, TableView tableView)
int size = tableView.columns.size();
matrix = new int[size];
for(int i=0; i<matrix.length; i++){
matrix[i] = -1;
}
for(int i=0; i<columns.size(); i++){
matrix[tableView.findColumnIdx(columns.get(i))] = i;
}
}
final void create(SSConnection con, Database database, TableView tableView) throws Exception{
init( database, tableView );
raFile = createFile( con, database );
}
static File getFile(Database database, String name) throws Exception{
return new File( Utils.createIdxFileName( database, name ) );
}
private FileChannel createFile(SSConnection con, Database database) throws Exception{
if( database.isReadOnly() ){
throw SmallSQLException.create(Language.DB_READONLY);
}
File file = getFile( database, name );
boolean ok = file.createNewFile();
if(!ok) throw SmallSQLException.create(Language.INDEX_EXISTS, name);
FileChannel randomFile = Utils.openRaFile( file, database.isReadOnly() );
con.add(new CreateFile(file, randomFile, con, database));
writeMagic(randomFile);
return randomFile;
}
private void load(Database database) throws SQLException{
try{
File file = getFile( database, name );
if(!file.exists())
throw SmallSQLException.create(Language.INDEX_MISSING, name);
raFile = Utils.openRaFile( file, database.isReadOnly() );
ByteBuffer buffer = ByteBuffer.allocate(8);
raFile.read(buffer);
buffer.position(0);
int magic   = buffer.getInt();
int version = buffer.getInt();
if(magic != MAGIC_INDEX){
throw SmallSQLException.create(Language.INDEX_FILE_INVALID, file.getName());
}
if(version > INDEX_VERSION){
Object[] params = { new Integer(version), file.getName() };
throw SmallSQLException.create(Language.FILE_TOONEW, params);
}
}catch(Throwable e){
if(raFile != null)
try{
raFile.close();
}catch(Exception e2){
DriverManager.println(e2.toString());
}
throw SmallSQLException.createFromException(e);
}
}
void drop(Database database) throws Exception {
close();
boolean ok = getFile( database, name).delete();
if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
}
void close() throws Exception{
if(raFile != null){
raFile.close();
raFile = null;
}
}
private final void writeMagic(FileChannel raFile) throws Exception{
ByteBuffer buffer = ByteBuffer.allocate(8);
buffer.putInt(MAGIC_INDEX);
buffer.putInt(INDEX_VERSION);
buffer.position(0);
raFile.write(buffer);
}
final void writeExpression( int columnIdx, Expression valueExpression) {
int idx = matrix[columnIdx];
if(idx >= 0) 
expressions.set(idx, valueExpression);
}
final void writeFinish(SSConnection con) {
}
final void save(StoreImpl store) throws SQLException{
store.writeInt(constraintType);
store.writeInt(columns.size());
for(int c=0; c<columns.size(); c++){
store.writeString( columns.get(c) );
}
store.writeString(name);
}
final static IndexDescription load(Database database, TableView tableView, StoreImpl store) throws SQLException{
int constraintType = store.readInt();
int count = store.readInt();
Strings columns = new Strings();
Expressions expressions = new Expressions();
SQLParser sqlParser = new SQLParser();
for(int c=0; c<count; c++){
String column = store.readString();
columns.add( column );
expressions.add( sqlParser.parseExpression(column));
}
IndexDescription indexDesc = new IndexDescription( store.readString(), tableView.name, constraintType, expressions, columns);
indexDesc.init( database, tableView );
indexDesc.load(database);
return indexDesc;
}
}
package smallsql.database;
final class ExpressionFunctionRound extends ExpressionFunctionReturnP1Number {
final int getFunction(){ return SQLTokenizer.ROUND; }
boolean isNull() throws Exception{
return param1.isNull() || param2.isNull();
}
final double getDouble() throws Exception{
if(isNull()) return 0;
final int places = param2.getInt();
double value = param1.getDouble();
long factor = 1;
if(places > 0){
for(int i=0; i<places; i++){
factor *= 10;
}
value *= factor;
}else{
for(int i=0; i>places; i--){
factor *= 10;
}
value /= factor;
}
value = Math.rint( value );
if(places > 0){
value /= factor;
}else{
value *= factor;
}
return value;
}
}
package smallsql.database;
import java.sql.*;
import java.util.ArrayList;
final class SSDatabaseMetaData implements DatabaseMetaData {
final private SSConnection con;
final private SSStatement st;
SSDatabaseMetaData(SSConnection con) throws SQLException{
this.con = con;
st = new SSStatement(con);
}
public boolean allProceduresAreCallable() {
return true;
}
public boolean allTablesAreSelectable() {
return true;
}
public String getURL() throws SQLException {
Database database = con.getDatabase(true);
if(database == null)
return SSDriver.URL_PREFIX;
return SSDriver.URL_PREFIX + ':' + database.getName();
}
public String getUserName() {
return "";
}
public boolean isReadOnly() {
return false;
}
public boolean nullsAreSortedHigh() {
return false;
}
public boolean nullsAreSortedLow() {
return true;
}
public boolean nullsAreSortedAtStart() {
return false;
}
public boolean nullsAreSortedAtEnd() {
return false;
}
public String getDatabaseProductName() {
return "SmallSQL Database";
}
public String getDatabaseProductVersion() {
return getDriverVersion();
}
public String getDriverName(){
return "SmallSQL Driver";
}
public String getDriverVersion() {
return getDriverMajorVersion() + "." + SSDriver.drv.getMinorVersion();
}
public int getDriverMajorVersion() {
return SSDriver.drv.getMajorVersion();
}
public int getDriverMinorVersion() {
return SSDriver.drv.getMinorVersion();
}
public boolean usesLocalFiles() {
return false;
}
public boolean usesLocalFilePerTable() {
return false;
}
public boolean supportsMixedCaseIdentifiers() {
return true;
}
public boolean storesUpperCaseIdentifiers() {
return false;
}
public boolean storesLowerCaseIdentifiers() {
return false;
}
public boolean storesMixedCaseIdentifiers() {
return true;
}
public boolean supportsMixedCaseQuotedIdentifiers() {
return true;
}
public boolean storesUpperCaseQuotedIdentifiers() {
return false;
}
public boolean storesLowerCaseQuotedIdentifiers() {
return false;
}
public boolean storesMixedCaseQuotedIdentifiers() {
return true;
}
public String getIdentifierQuoteString() {
return "\"";
}
public String getSQLKeywords() {
return "database,use";
}
private String getFunctions(int from, int to){
StringBuffer buf = new StringBuffer();
for(int i=from; i<=to; i++){
if(i != from) buf.append(',');
buf.append( SQLTokenizer.getKeyWord(i) );
}
return buf.toString();
}
public String getNumericFunctions() {
return getFunctions(SQLTokenizer.ABS, SQLTokenizer.TRUNCATE);
}
public String getStringFunctions() {
return getFunctions(SQLTokenizer.ASCII, SQLTokenizer.UCASE);
}
public String getSystemFunctions() {
return getFunctions(SQLTokenizer.IFNULL, SQLTokenizer.IIF);
}
public String getTimeDateFunctions() {
return getFunctions(SQLTokenizer.CURDATE, SQLTokenizer.YEAR);
}
public String getSearchStringEscape() {
return "\\";
}
public String getExtraNameCharacters() {
return "#$ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ";
}
public boolean supportsAlterTableWithAddColumn() {
throw new java.lang.UnsupportedOperationException("Method supportsAlterTableWithDropColumn() not yet implemented.");
}
public boolean supportsColumnAliasing() {
return true;
}
public boolean nullPlusNonNullIsNull() {
return true;
}
public boolean supportsConvert() {
return true;
}
public boolean supportsConvert(int fromType, int toType) {
return true;
}
public boolean supportsTableCorrelationNames() {
return true;
}
public boolean supportsDifferentTableCorrelationNames() {
return true;
}
public boolean supportsExpressionsInOrderBy() {
return true;
}
public boolean supportsOrderByUnrelated() {
return true;
}
public boolean supportsGroupBy() {
return true;
}
public boolean supportsGroupByUnrelated() {
return true;
}
public boolean supportsGroupByBeyondSelect() {
return true;
}
public boolean supportsLikeEscapeClause() {
return true;
}
public boolean supportsMultipleResultSets() {
return true;
}
public boolean supportsMultipleTransactions() {
return true;
}
public boolean supportsNonNullableColumns() {
return true;
}
public boolean supportsMinimumSQLGrammar() {
return true;
}
public boolean supportsCoreSQLGrammar() {
return true;
}
public boolean supportsExtendedSQLGrammar() {
return true;
}
public boolean supportsANSI92EntryLevelSQL() {
return true;
}
public boolean supportsANSI92IntermediateSQL() {
return true;
}
public boolean supportsANSI92FullSQL() {
return true;
}
public boolean supportsIntegrityEnhancementFacility() {
return true;
}
public boolean supportsOuterJoins() {
return true;
}
public boolean supportsFullOuterJoins() {
return true;
}
public boolean supportsLimitedOuterJoins() {
return true;
}
public String getSchemaTerm() {
return "owner";
}
public String getProcedureTerm() {
return "procedure";
}
public String getCatalogTerm() {
return "database";
}
public boolean isCatalogAtStart() {
return true;
}
public String getCatalogSeparator() {
return ".";
}
public boolean supportsSchemasInDataManipulation() {
return false;
}
public boolean supportsSchemasInProcedureCalls() {
return false;
}
public boolean supportsSchemasInTableDefinitions() {
return false;
}
public boolean supportsSchemasInIndexDefinitions() {
return false;
}
public boolean supportsSchemasInPrivilegeDefinitions() {
return false;
}
public boolean supportsCatalogsInDataManipulation() {
return true;
}
public boolean supportsCatalogsInProcedureCalls() {
return true;
}
public boolean supportsCatalogsInTableDefinitions() {
return true;
}
public boolean supportsCatalogsInIndexDefinitions() {
return true;
}
public boolean supportsCatalogsInPrivilegeDefinitions() {
return true;
}
public boolean supportsPositionedDelete() {
return true;
}
public boolean supportsPositionedUpdate() {
return true;
}
public boolean supportsSelectForUpdate() {
return true;
}
public boolean supportsStoredProcedures() {
return false;
}
public boolean supportsSubqueriesInComparisons() {
return true;
}
public boolean supportsSubqueriesInExists() {
return true;
}
public boolean supportsSubqueriesInIns() {
return true;
}
public boolean supportsSubqueriesInQuantifieds() {
return true;
}
public boolean supportsCorrelatedSubqueries() {
return true;
}
public boolean supportsUnion() {
return true;
}
public boolean supportsUnionAll() {
return true;
}
public boolean supportsOpenCursorsAcrossCommit() {
return true;
}
public boolean supportsOpenCursorsAcrossRollback() {
return true;
}
public boolean supportsOpenStatementsAcrossCommit() {
return true;
}
public boolean supportsOpenStatementsAcrossRollback() {
return true;
}
public int getMaxBinaryLiteralLength() {
return 0;
}
public int getMaxCharLiteralLength() {
return 0;
}
public int getMaxColumnNameLength() {
return 255;
}
public int getMaxColumnsInGroupBy() {
return 0;
}
public int getMaxColumnsInIndex() {
return 0;
}
public int getMaxColumnsInOrderBy() {
return 0;
}
public int getMaxColumnsInSelect() {
return 0;
}
public int getMaxColumnsInTable() {
return 0;
}
public int getMaxConnections() {
return 0;
}
public int getMaxCursorNameLength() {
return 0;
}
public int getMaxIndexLength() {
return 0;
}
public int getMaxSchemaNameLength() {
return 255;
}
public int getMaxProcedureNameLength() {
return 255;
}
public int getMaxCatalogNameLength() {
return 255;
}
public int getMaxRowSize() {
return 0;
}
public boolean doesMaxRowSizeIncludeBlobs() {
return false;
}
public int getMaxStatementLength() {
return 0;
}
public int getMaxStatements() {
return 0;
}
public int getMaxTableNameLength() {
return 255;
}
public int getMaxTablesInSelect() {
return 0;
}
public int getMaxUserNameLength() {
return 0;
}
public int getDefaultTransactionIsolation() {
return Connection.TRANSACTION_READ_COMMITTED;
}
public boolean supportsTransactions() {
return true;
}
public boolean supportsTransactionIsolationLevel(int level) {
switch(level){
case Connection.TRANSACTION_NONE:
case Connection.TRANSACTION_READ_UNCOMMITTED:
case Connection.TRANSACTION_READ_COMMITTED:
case Connection.TRANSACTION_REPEATABLE_READ:
case Connection.TRANSACTION_SERIALIZABLE:
return true;
}
return false;
}
public boolean supportsDataDefinitionAndDataManipulationTransactions() {
return true;
}
public boolean supportsDataManipulationTransactionsOnly() {
return false;
}
public boolean dataDefinitionCausesTransactionCommit() {
return false;
}
public boolean dataDefinitionIgnoredInTransactions() {
return false;
}
public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS", "PROCEDURE_TYPE"};
Object[][] data   = new Object[0][];
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS" };
Object[][] data   = new Object[0][];
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
String[] colNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS","TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"};
Database database;
if(catalog == null){
database = con.getDatabase(true);
if(database != null)
catalog = database.getName();
}else{
database = Database.getDatabase(catalog, con, false);
}
ArrayList rows = new ArrayList();
boolean isTypeTable = types == null;
boolean isTypeView = types == null;
for(int i=0; types != null && i<types.length; i++){
if("TABLE".equalsIgnoreCase(types[i])) isTypeTable = true;
if("VIEW" .equalsIgnoreCase(types[i])) isTypeView  = true;
}
if(database != null){
Strings tables = database.getTables(tableNamePattern);
for(int i=0; i<tables.size(); i++){
String table = tables.get(i);
Object[] row = new Object[10];
row[0] = catalog;
row[2] = table;
try{
if(database.getTableView( con, table) instanceof View){
if(isTypeView){
row[3] = "VIEW";
rows.add(row);
}
}else{
if(isTypeTable){
row[3] = "TABLE";
rows.add(row);
}
}
}catch(Exception e){
}
}
}
Object[][] data = new Object[rows.size()][];
rows.toArray(data);
CommandSelect cmdSelect = Utils.createMemoryCommandSelect( con, colNames, data);
Expressions order = new Expressions();
order.add( new ExpressionName("TABLE_TYPE") );
order.add( new ExpressionName("TABLE_NAME") );
cmdSelect.setOrder( order );
return new SSResultSet( st, cmdSelect);
}
public ResultSet getSchemas() throws SQLException {
String[] colNames = {"TABLE_SCHEM"};
Object[][] data   = new Object[0][];
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public ResultSet getCatalogs() throws SQLException {
String[] colNames = {"TABLE_CAT"};
Object[][] data   = Database.getCatalogs(con.getDatabase(true));
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public ResultSet getTableTypes() throws SQLException {
String[] colNames = {"TABLE_TYPE"};
Object[][] data   = {{"SYSTEM TABLE"}, {"TABLE"}, {"VIEW"}};
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
try {
String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"};
Object[][] data   = con.getDatabase(false).getColumns(con, tableNamePattern, columnNamePattern);
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
throw new java.lang.UnsupportedOperationException("Method getTablePrivileges() not yet implemented.");
}
public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
try {
String[] colNames = {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
Object[][] data   = con.getDatabase(false).getBestRowIdentifier(con, table);
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
try {
String[] colNames = {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
Object[][] data   = new Object[0][0];
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
try {
String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
Object[][] data   = con.getDatabase(false).getPrimaryKeys(con, table);
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
return getCrossReference( null, null, null, null, null, table );
}
public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
return getCrossReference( null, null, table, null, null, null );
}
public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
try {
String[] colNames = {"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
Object[][] data   = con.getDatabase(false).getReferenceKeys(con, primaryTable, foreignTable);
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public ResultSet getTypeInfo() throws SQLException {
String[] colNames = {		"TYPE_NAME", 				"DATA_TYPE", 																	"PRECISION", 	"LITERAL_PREFIX", "LITERAL_SUFFIX", 		"CREATE_PARAMS", "NULLABLE", 	 "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
Object[][] data   = {
{SQLTokenizer.getKeyWord(SQLTokenizer.UNIQUEIDENTIFIER),Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.UNIQUEIDENTIFIER)), Utils.getInteger(36),      	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null,          Boolean.FALSE, Boolean.FALSE, null, null,                null,                null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.BIT),             Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BIT) ),             Utils.getInteger(1),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null,          Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.TINYINT),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.TINYINT) ),         Utils.getInteger(3),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.TRUE,  Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.BIGINT),          Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BIGINT) ),          Utils.getInteger(19),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.LONGVARBINARY),   Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.LONGVARBINARY) ),   Utils.getInteger(2147483647),	"0x", null, null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.VARBINARY),   	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.VARBINARY) ),   	  Utils.getInteger(65535),	    "0x", null, "max length", 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.BINARY),   	 	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BINARY) ),   	  	  Utils.getInteger(65535),	    "0x", null, "length", 			Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.LONGVARCHAR),     Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.LONGVARCHAR) ),     Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.LONGNVARCHAR),    Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.LONGNVARCHAR) ),    Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.CHAR),         	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.CHAR) ),         	  Utils.getInteger(65535),   	"'",  "'",  "length", 			Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.NCHAR),         	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NCHAR) ),           Utils.getInteger(65535),   	"'",  "'",  "length", 			Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.NUMERIC),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NUMERIC) ),         Utils.getInteger(38),     	null, null, "precision,scale", 	Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(38),null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.DECIMAL),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.DECIMAL) ),         Utils.getInteger(38),     	null, null, "precision,scale", 	Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(38),null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.MONEY),           Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.MONEY) ),           Utils.getInteger(19),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(4), Utils.getInteger(4), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.SMALLMONEY),      Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.SMALLMONEY) ),      Utils.getInteger(10),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(4), Utils.getInteger(4), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.INT),             Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.INT) ),             Utils.getInteger(10),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.SMALLINT),        Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.SMALLINT) ),        Utils.getInteger(5),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.FLOAT),        	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.FLOAT) ),           Utils.getInteger(15),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.REAL),        	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.REAL) ),        	  Utils.getInteger(7),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.DOUBLE),          Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.DOUBLE) ),          Utils.getInteger(15),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.VARCHAR),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.VARCHAR) ),         Utils.getInteger(65535),   	"'",  "'",  "max length", 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.NVARCHAR),        Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NVARCHAR) ),        Utils.getInteger(65535),   	"'",  "'",  "max length", 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.BOOLEAN),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BOOLEAN) ),         Utils.getInteger(1),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null,          Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.DATE),   	 	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.DATE) ), 	  		  Utils.getInteger(10),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.TIME),   	 	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.TIME) ), 	  		  Utils.getInteger(8),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.TIMESTAMP),   	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.TIMESTAMP) ), 	  Utils.getInteger(23),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(3), Utils.getInteger(3), null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.SMALLDATETIME),   Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.SMALLDATETIME) ),   Utils.getInteger(16),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.JAVA_OBJECT),   	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.JAVA_OBJECT) ),     Utils.getInteger(65535),	    null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.BLOB),   		 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BLOB) ),   		  Utils.getInteger(2147483647),	"0x", null, null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.CLOB),     		 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.CLOB) ),     		  Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
{SQLTokenizer.getKeyWord(SQLTokenizer.NCLOB),     		 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NCLOB) ),     	  Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
};
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
try {
String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"};
Object[][] data   = con.getDatabase(false).getIndexInfo(con, table, unique);
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
} catch (Exception e) {
throw SmallSQLException.createFromException(e);
}
}
public boolean supportsResultSetType(int type) {
switch(type){
case ResultSet.TYPE_FORWARD_ONLY:
case ResultSet.TYPE_SCROLL_INSENSITIVE:
case ResultSet.TYPE_SCROLL_SENSITIVE:
return true;
}
return false;
}
public boolean supportsResultSetConcurrency(int type, int concurrency) {
if(type >= ResultSet.TYPE_FORWARD_ONLY && type <= ResultSet.TYPE_SCROLL_SENSITIVE &&
concurrency >= ResultSet.CONCUR_READ_ONLY && concurrency <= ResultSet.CONCUR_UPDATABLE)
return true;
return false;
}
public boolean ownUpdatesAreVisible(int type) {
return supportsResultSetType(type);
}
public boolean ownDeletesAreVisible(int type) {
return supportsResultSetType(type);
}
public boolean ownInsertsAreVisible(int type) {
return supportsResultSetType(type);
}
public boolean othersUpdatesAreVisible(int type) {
return supportsResultSetType(type);
}
public boolean othersDeletesAreVisible(int type) {
return supportsResultSetType(type);
}
public boolean othersInsertsAreVisible(int type) {
return supportsResultSetType(type);
}
public boolean updatesAreDetected(int type) {
return false;
}
public boolean deletesAreDetected(int type) {
return supportsResultSetType(type);
}
public boolean insertsAreDetected(int type) {
return supportsResultSetType(type);
}
public boolean supportsBatchUpdates() {
return true;
}
public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
String[] colNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS"};
Object[][] data   = new Object[0][];
return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
}
public Connection getConnection() {
return con;
}
public boolean supportsSavepoints() {
return false;
}
public boolean supportsNamedParameters() {
return true;
}
public boolean supportsMultipleOpenResults() {
return true;
}
public boolean supportsGetGeneratedKeys() {
return true;
}
public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
throw new java.lang.UnsupportedOperationException("Method getSuperTables() not yet implemented.");
}
public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
package smallsql.database;
final class ExpressionFunctionPI extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.PI; }
boolean isNull() throws Exception{
return false;
}
final double getDouble() throws Exception{
return Math.PI;
}
}
package smallsql.database;
import java.io.ByteArrayOutputStream;
import smallsql.database.language.Language;
public class ExpressionFunctionInsert extends ExpressionFunctionReturnP1StringAndBinary {
final int getFunction() {
return SQLTokenizer.INSERT;
}
final boolean isNull() throws Exception {
return param1.isNull() || param2.isNull() || param3.isNull() || param4.isNull();
}
final byte[] getBytes() throws Exception{
if(isNull()) return null;
byte[] bytes = param1.getBytes();
int start  = Math.min(Math.max( 0, param2.getInt() - 1), bytes.length );
int length = Math.min(param3.getInt(), bytes.length );
ByteArrayOutputStream buffer = new ByteArrayOutputStream();
buffer.write(bytes,0,start);
buffer.write(param4.getBytes());
if(length < 0)
throw SmallSQLException.create(Language.INSERT_INVALID_LEN, new Integer(length));
buffer.write(bytes, start+length, bytes.length-start-length);
return buffer.toByteArray();
}
final String getString() throws Exception {
if(isNull()) return null;
String str = param1.getString();
int start  = Math.min(Math.max( 0, param2.getInt() - 1), str.length() );
int length = Math.min(param3.getInt(), str.length() );
StringBuffer buffer = new StringBuffer();
buffer.append(str.substring(0,start));
buffer.append(param4.getString());
if(length < 0)
throw SmallSQLException.create(Language.INSERT_INVALID_LEN, new Integer(length));
buffer.append(str.substring(start+length));
return buffer.toString();
}
int getPrecision() {
return param1.getPrecision()+param2.getPrecision();
}
}
package smallsql.junit;
import java.sql.*;
public class TestOther extends BasicTestCase {
public void testInsertSelect() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table InsertSelect (i counter, v varchar(20))");
assertEqualsRsValue( new Integer(0), "Select count(*) from InsertSelect");
con.createStatement().execute("Insert Into InsertSelect(v) Values('qwert')");
assertEqualsRsValue( new Integer(1), "Select count(*) from InsertSelect");
con.createStatement().execute("Insert Into InsertSelect(v) Select v From InsertSelect");
assertEqualsRsValue( new Integer(2), "Select count(*) from InsertSelect");
con.createStatement().execute("Insert Into InsertSelect(v) (Select v From InsertSelect)");
assertEqualsRsValue( new Integer(4), "Select count(*) from InsertSelect");
}finally{
dropTable( con, "InsertSelect" );
}
}
public void testDistinct() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table TestDistinct (i counter, v varchar(20), n bigint, b boolean)");
assertRowCount( 0, "Select * From TestDistinct" );
con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert1',true)");
con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert2',true)");
con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert1',true)");
con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert2',true)");
con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert1',false)");
assertRowCount( 5, "Select b,n,v From TestDistinct" );
assertRowCount( 2, "Select Distinct v From TestDistinct t1" );
assertRowCount( 3, "Select Distinct b,n,v From TestDistinct" );
assertRowCount( 3, "Select Distinct b,n,v,i+null,23+i-i,'asdf'+v From TestDistinct" );
assertRowCount( 5, "Select All b,n,v From TestDistinct" );
}finally{
dropTable( con, "TestDistinct" );
}
}
public void testConstantAndRowPos() throws Exception{
assertRowCount( 1, "Select 12, 'qwert'" );
}
public void testNoFromResult() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY );
ResultSet rs = st.executeQuery("Select 12, 'qwert' alias");
assertRSMetaData( rs, new String[]{"col1", "alias"}, new int[]{Types.INTEGER, Types.VARCHAR });
assertTrue( rs.isBeforeFirst() );
assertFalse( rs.isFirst() );
assertFalse( rs.isLast() );
assertFalse( rs.isAfterLast() );
assertTrue( rs.next() );
assertFalse( rs.isBeforeFirst() );
assertTrue( rs.isFirst() );
assertTrue( rs.isLast() );
assertFalse( rs.isAfterLast() );
assertFalse( rs.next() );
assertFalse( rs.isBeforeFirst() );
assertFalse( rs.isFirst() );
assertFalse( rs.isLast() );
assertTrue( rs.isAfterLast() );
assertTrue( rs.previous() );
assertFalse( rs.isBeforeFirst() );
assertTrue( rs.isFirst() );
assertTrue( rs.isLast() );
assertFalse( rs.isAfterLast() );
assertFalse( rs.previous() );
assertTrue( rs.isBeforeFirst() );
assertFalse( rs.isFirst() );
assertFalse( rs.isLast() );
assertFalse( rs.isAfterLast() );
assertTrue( rs.first() );
assertFalse( rs.isBeforeFirst() );
assertTrue( rs.isFirst() );
assertTrue( rs.isLast() );
assertFalse( rs.isAfterLast() );
assertTrue( rs.last() );
assertFalse( rs.isBeforeFirst() );
assertTrue( rs.isFirst() );
assertTrue( rs.isLast() );
assertFalse( rs.isAfterLast() );
}
public void testInSelect() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table TestInSelect (i counter, v varchar(20), n bigint, b boolean)");
assertRowCount( 0, "Select * From TestInSelect WHere i In (Select i from TestInSelect)" );
con.createStatement().execute("Insert Into TestInSelect(v,b) Values('qwert1',true)");
assertRowCount( 1, "Select * From TestInSelect WHere i In (Select i from TestInSelect)" );
con.createStatement().execute("Insert Into TestInSelect(v,b) Values('qwert1',true)");
assertRowCount( 2, "Select * From TestInSelect WHere i In (Select i from TestInSelect)" );
assertRowCount( 1, "Select * From TestInSelect WHere i In (Select i from TestInSelect Where i>1)" );
assertRowCount( 1, "Select * From TestInSelect Where i IN ( 1, 1, 12345, 987654321)" );
assertRowCount( 2, "Select * From TestInSelect Where v IN ( null, '', 'qwert1', 'qwert1')" );
assertRowCount( 2, "Select * From TestInSelect Where v IN ( 'qwert1')" );
assertRowCount( 0, "Select * From TestInSelect Where '' IN ( 'qwert1')" );
assertRowCount( 2, "Select * From TestInSelect Where 'qwert1' IN ( 'qwert1', 'qwert2')" );
}finally{
dropTable( con, "TestInSelect" );
}
}
public void testSetTransaction() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Set Transaction Isolation Level Read Uncommitted");
assertEquals( Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation() );
con.createStatement().execute("Set Transaction Isolation Level Read Committed");
assertEquals( Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation() );
con.createStatement().execute("Set Transaction Isolation Level Repeatable Read");
assertEquals( Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation() );
con.createStatement().execute("Set Transaction Isolation Level Serializable");
assertEquals( Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation() );
}finally{
con.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
}
}
public void testCreateDropDatabases() throws Exception{
Connection con = DriverManager.getConnection("jdbc:smallsql");
Statement st = con.createStatement();
try{
st.execute("Create Database anyTestDatabase");
}catch(SQLException ex){
st.execute("Drop Database anyTestDatabase");
throw ex;
}
st.execute("Drop Database anyTestDatabase");
}
public void testManyColumns() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
dropTable( con, "ManyCols" );
StringBuffer buf = new StringBuffer("Create Table ManyCols(");
for(int i=1; i<300; i++){
if(i!=1)buf.append(',');
buf.append("column").append(i).append(" int");
}
buf.append(')');
st.execute(buf.toString());
con.close();
con = AllTests.getConnection();
st = con.createStatement();
assertEquals(1,st.executeUpdate("Insert Into ManyCols(column260) Values(123456)"));
st.execute("Drop Table ManyCols");
}
public void testCharEqualsVarchar() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table CharEqualsVarchar (c char(10))");
assertRowCount( 0, "Select * From CharEqualsVarchar" );
con.createStatement().execute("Insert Into CharEqualsVarchar(c) Values('qwert1')");
assertRowCount( 1, "Select * From CharEqualsVarchar" );
assertRowCount( 1, "Select * From CharEqualsVarchar Where c = 'qwert1'" );
assertRowCount( 0, "Select * From CharEqualsVarchar Where c = 'qwert1        xxxx'" );
assertRowCount( 1, "Select * From CharEqualsVarchar Where c = cast('qwert1' as char(8))" );
assertRowCount( 1, "Select * From CharEqualsVarchar Where c = cast('qwert1' as char(12))" );
assertRowCount( 1, "Select * From CharEqualsVarchar Where c In('qwert1')" );
assertRowCount( 0, "Select * From CharEqualsVarchar Where c In('qwert1        xxxx')" );
PreparedStatement pr;
pr = con.prepareStatement( "Select * From CharEqualsVarchar Where c = ?" );
pr.setString( 1, "qwert1" );
assertRowCount( 1, pr.executeQuery() );
pr.setString( 1, "qwert1        xxxx" );
assertRowCount( 0, pr.executeQuery() );
}finally{
dropTable( con, "CharEqualsVarchar" );
}
}
public void testLike() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table Like (c varchar(20))");
con.createStatement().execute("Insert Into Like(c) Values('qwert1')");
con.createStatement().execute("Insert Into Like(c) Values('qwert2')");
con.createStatement().execute("Insert Into Like(c) Values('qwert2.5')");
con.createStatement().execute("Insert Into Like(c) Values('awert1')");
con.createStatement().execute("Insert Into Like(c) Values('awert2')");
con.createStatement().execute("Insert Into Like(c) Values('awert3')");
con.createStatement().execute("Insert Into Like(c) Values('qweSGSGSrt1')");
assertRowCount( 2, "Select * From Like Where c like 'qwert_'" );
assertRowCount( 3, "Select * From Like Where c like 'qwert%'" );
assertRowCount( 2, "Select * From Like Where c like 'qwert2%'" );
assertRowCount( 6, "Select * From Like Where c like '_wert%'" );
assertRowCount( 2, "Select * From Like Where c like 'qwe%rt1'" );
assertRowCount( 3, "Select * From Like Where c like 'qwe%rt_'" );
assertRowCount( 7, "Select * From Like Where c like '%_'" );
}finally{
dropTable( con, "Like" );
}
}
public void testBinaryStore() throws Exception{
Connection con = AllTests.getConnection();
try{
Statement st = con.createStatement();
st.execute("Create Table Binary (b varbinary(20))");
st.execute("Truncate Table Binary");
st.execute("Insert Into Binary(b) Values(12345)");
ResultSet rs = st.executeQuery("Select * From Binary");
rs.next();
assertEquals(rs.getInt(1), 12345);
st.execute("Truncate Table Binary");
st.execute("Insert Into Binary(b) Values(1.2345)");
rs = st.executeQuery("Select * From Binary");
rs.next();
assertEquals( 1.2345, rs.getDouble(1), 0.0);
st.execute("Truncate Table Binary");
st.execute("Insert Into Binary(b) Values(cast(1.2345 as real))");
rs = st.executeQuery("Select * From Binary");
rs.next();
assertEquals( 1.2345F, rs.getFloat(1), 0.0);
}finally{
dropTable( con, "Binary" );
}
}
public void testCatalog() throws Exception{
Connection con = DriverManager.getConnection("jdbc:smallsql");
assertEquals( "", con.getCatalog() );
con.setCatalog( AllTests.CATALOG );
assertEquals( AllTests.CATALOG, con.getCatalog() );
con.close();
con = DriverManager.getConnection("jdbc:smallsql");
assertEquals( "", con.getCatalog() );
con.createStatement().execute( "Use " + AllTests.CATALOG );
assertEquals( AllTests.CATALOG, con.getCatalog() );
con.close();
con = DriverManager.getConnection("jdbc:smallsql?dbpath=" + AllTests.CATALOG);
assertEquals( AllTests.CATALOG, con.getCatalog() );
con.close();
}
}
package smallsql.database;
import java.util.*;
import java.sql.SQLException;
import java.sql.Types;
import smallsql.database.language.Language;
public class SQLTokenizer {
private static final int NOT_COMMENT = 0;
private static final int LINE_COMMENT = 1;
private static final int MULTI_COMMENT = 2;
public static List parseSQL( char[] sql ) throws SQLException{
SearchNode node = searchTree;
ArrayList tokens = new ArrayList();
int value = 0;
int tokenStart = 0;
boolean wasWhiteSpace = true;
int comment = NOT_COMMENT;
char quote = 0;
StringBuffer quoteBuffer = new StringBuffer();
for(int i=0; i<sql.length; i++){
char c = sql[i];
switch(c){
case '\"':
case '\'':
if (comment != NOT_COMMENT) {
break;
}else if(quote == 0){
quote = c;
}else if(quote == c){
if(i+1<sql.length && sql[i+1] == quote){
quoteBuffer.append(quote);
i++;
}else{
tokens.add( new SQLToken( quoteBuffer.toString(), (quote == '\'') ? STRING : IDENTIFIER,       tokenStart, i+1) );
quoteBuffer.setLength(0);
quote = 0;
tokenStart = i+1;
wasWhiteSpace = true;
}
}else quoteBuffer.append(c);
break;
case '.':
if (comment != NOT_COMMENT) {
break;
}else if(quote == 0){
int k=tokenStart;
if(k == i){ 
if(sql.length> k+1){
char cc = sql[k+1];
if((cc >= '0') && cc <= '9') break; 
}
}else{
for(; k<i; k++){
char cc = sql[k];
if((cc != '-' && cc != '$' && cc < '0') || cc > '9') break; 
}
if(k>=i) break; 
}
}
case '-':
if (comment != NOT_COMMENT) {
break;
}
if (comment == LINE_COMMENT) {
if (c == '\r' || c == '\n') {
comment = NOT_COMMENT;
wasWhiteSpace = true;
}
tokenStart = i+1;
break;
}
case '/':
if((i+1 < sql.length) && (sql[i+1] == '*')){
i++;
tokenStart = i+1;
comment = MULTI_COMMENT;
break;
}
default:
tokens.add( new SQLToken( c, i, i+1) );
}
wasWhiteSpace = true;
tokenStart = i+1;
}else{
quoteBuffer.append(c);
}
break;
default:
if (comment != NOT_COMMENT) {
break;
}else if(quote == 0){
if(wasWhiteSpace){
node = searchTree;
}else{
if(node == null){
value = 0;
wasWhiteSpace = false;
break;
}
}
c |= 0x20; 
while(node != null && node.letter != c) node = node.nextEntry;
if(node != null){
value = node.value;
node = node.nextLetter;
}else{
value = 0;
node = null;
}
}else{
quoteBuffer.append(c);
}
wasWhiteSpace = false;
break;
}
}
if (comment == MULTI_COMMENT) {
throw SmallSQLException.create(Language.STXADD_COMMENT_OPEN);
}
if(!wasWhiteSpace) {
tokens.add( new SQLToken( value, tokenStart, sql.length) );
}
return tokens;
}
static private void addKeyWord( String keyword, int value){
keywords.put( Utils.getInteger( value), keyword );
char[] letters = keyword.toCharArray();
if(searchTree == null){
searchTree = new SearchNode();
searchTree.letter = (char)(letters[0] | 0x20);
}
SearchNode prev = null;
SearchNode node = searchTree;
boolean wasNextEntry = true;
for(int i=0; i<letters.length; i++){
char c = (char)(letters[i] | 0x20);
while(node != null && node.letter != c) {
prev = node;
node = node.nextEntry;
wasNextEntry = true;
}
if(node == null){
node = new SearchNode();
node.letter = c;
if(wasNextEntry)
prev.nextEntry = node;
else prev.nextLetter = node;
wasNextEntry = false;
prev = node;
node = null;
}else{
prev = node;
node = node.nextLetter;
wasNextEntry = false;
}
}
prev.value = value;
}
static final String getKeyWord(int key){
return (String)keywords.get( Utils.getInteger(key) );
}
static final int getSQLDataType(int type){
switch(type){
case SQLTokenizer.BIT:
return Types.BIT;
case SQLTokenizer.BOOLEAN:
return Types.BOOLEAN;
case SQLTokenizer.BINARY:
return Types.BINARY;
case SQLTokenizer.VARBINARY:
return Types.VARBINARY;
case SQLTokenizer.LONGVARBINARY:
return Types.LONGVARBINARY;
case SQLTokenizer.BLOB:
return Types.BLOB;
case SQLTokenizer.TINYINT:
return Types.TINYINT;
case SQLTokenizer.SMALLINT:
return Types.SMALLINT;
case SQLTokenizer.INT:
return Types.INTEGER;
case SQLTokenizer.BIGINT:
return Types.BIGINT;
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.MONEY:
case SQLTokenizer.DECIMAL:
return Types.DECIMAL;
case SQLTokenizer.NUMERIC:
return Types.NUMERIC;
case SQLTokenizer.REAL:
return Types.REAL;
case SQLTokenizer.FLOAT:
return Types.FLOAT;
case SQLTokenizer.DOUBLE:
return Types.DOUBLE;
case SQLTokenizer.DATE:
return Types.DATE;
case SQLTokenizer.TIME:
return Types.TIME;
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
return Types.TIMESTAMP;
case SQLTokenizer.CHAR:
case SQLTokenizer.NCHAR:
return Types.CHAR;
case SQLTokenizer.VARCHAR:
case SQLTokenizer.NVARCHAR:
return Types.VARCHAR;
case SQLTokenizer.LONGNVARCHAR:
case SQLTokenizer.LONGVARCHAR:
return Types.LONGVARCHAR;
case SQLTokenizer.CLOB:
case SQLTokenizer.NCLOB:
return Types.CLOB;
case SQLTokenizer.JAVA_OBJECT:
return Types.JAVA_OBJECT;
case SQLTokenizer.UNIQUEIDENTIFIER:
return -11;
case SQLTokenizer.NULL:
return Types.NULL;
default: throw new Error("DataType:"+type);
}
}
static SearchNode searchTree;
static Hashtable keywords = new Hashtable(337);
static final int VALUE      = 0;
static final int STRING     = 3;
static final int IDENTIFIER  = 4;
static final int NUMBERVALUE= 5;
static{
keywords.put( new Integer(VALUE),       "<expression>" );
keywords.put( new Integer(IDENTIFIER),   "<identifier>" );
keywords.put( new Integer(NUMBERVALUE), "<number>" );
}
static final int PERCENT        = '%'; 
static final int BIT_AND        = '&'; 
static final int PARENTHESIS_L  = '('; 
static final int PARENTHESIS_R  = ')'; 
static final int ASTERISK       = '*'; 
static final int PLUS           = '+'; 
static final int COMMA          = ','; 
static final int MINUS          = '-'; 
static final int POINT          = '.'; 
static final int SLACH          = '/'; 
static final int LESSER         = '<'; 
static final int EQUALS         = '='; 
static final int GREATER        = '>'; 
static final int QUESTION       = '?'; 
static final int BIT_XOR        = '^'; 
static final int ESCAPE_L       = '{'; 
static final int BIT_OR         = '|'; 
static final int ESCAPE_R       = '}'; 
static final int TILDE          = '~'; 
static final int LESSER_EQU     = 100 + LESSER; 
static final int UNEQUALS       = 100 + EQUALS;  
static final int GREATER_EQU    = 100 + GREATER; 
static{
keywords.put( new Integer(LESSER_EQU),	"<=" );
keywords.put( new Integer(UNEQUALS),   	"<>" );
keywords.put( new Integer(GREATER_EQU), ">=" );
}
static final int SELECT     = 200;
static final int DELETE     = 201;
static final int INTO       = 203;
static final int UPDATE     = 204;
static final int CREATE     = 205;
static final int DROP       = 206;
static final int ALTER      = 207;
static final int SET        = 208;
static final int EXECUTE    = 209;
static final int FROM       = 210;
static final int WHERE      = 211;
static final int GROUP      = 212;
static final int BY         = 213;
static final int HAVING     = 214;
static final int ORDER      = 215;
static final int ASC        = 216;
static final int DESC       = 217;
static final int VALUES     = 218;
static final int AS         = 219;
static final int DEFAULT    = 220;
static final int IDENTITY   = 221;
static final int INNER      = 222;
static final int JOIN       = 223;
static final int ON         = 224;
static final int OUTER      = 225;
static final int FULL       = 226;
static final int CROSS      = 227;
static final int USE		= 228;
static final int TOP		= 229;
static final int ADD		= 230;
static final int LIMIT      = 231;
static final int DATABASE   = 235;
static final int TABLE      = 236;
static final int VIEW       = 237;
static final int INDEX      = 238;
static final int PROCEDURE  = 239;
static final int TRANSACTION= 240;
static final int ISOLATION  = 241;
static final int LEVEL      = 242;
static final int READ       = 243;
static final int COMMITTED  = 244;
static final int UNCOMMITTED= 245;
static final int REPEATABLE = 246;
static final int SERIALIZABLE= 247;
static final int CONSTRAINT = 250;
static final int PRIMARY 	= 251;
static final int FOREIGN 	= 252;
static final int KEY 		= 253;
static final int UNIQUE 	= 254;
static final int CLUSTERED  = 255;
static final int NONCLUSTERED=256;
static final int REFERENCES = 257;
static final int UNION 		= 260;
static final int ALL 		= 261;
static final int DISTINCT   = 262;
static final int CASE 		= 263;
static final int WHEN 		= 264;
static final int THEN 		= 265;
static final int ELSE 		= 266;
static final int END 		= 267;
static final int SWITCH 	= 268;
static final String DESC_STR   = "DESC";
static{
addKeyWord( "SELECT",   SELECT);
addKeyWord( "DELETE",   DELETE);
addKeyWord( "INTO",     INTO);
addKeyWord( "UPDATE",   UPDATE);
addKeyWord( "CREATE",   CREATE);
addKeyWord( "DROP",     DROP);
addKeyWord( "ALTER",    ALTER);
addKeyWord( "SET",      SET);
addKeyWord( "EXEC",     EXECUTE); 
addKeyWord( "EXECUTE",  EXECUTE);
addKeyWord( "FROM",     FROM);
addKeyWord( "WHERE",    WHERE);
addKeyWord( "GROUP",    GROUP);
addKeyWord( "BY",       BY);
addKeyWord( "HAVING",   HAVING);
addKeyWord( "ORDER",    ORDER);
addKeyWord( "ASC",      ASC);
addKeyWord( DESC_STR,   DESC);
addKeyWord( "VALUES",   VALUES);
addKeyWord( "AS",       AS);
addKeyWord( "DEFAULT",  DEFAULT);
addKeyWord( "AUTO_INCREMENT", IDENTITY); 
addKeyWord( "IDENTITY", IDENTITY);
addKeyWord( "INNER",    INNER);
addKeyWord( "JOIN",     JOIN);
addKeyWord( "ON",       ON);
addKeyWord( "OUTER",    OUTER);
addKeyWord( "FULL",     FULL);
addKeyWord( "CROSS",    CROSS);
addKeyWord( "USE",      USE);
addKeyWord( "TOP",      TOP);
addKeyWord( "ADD",      ADD);
addKeyWord( "LIMIT",    LIMIT);
addKeyWord( "DATABASE", DATABASE);
addKeyWord( "TABLE",    TABLE);
addKeyWord( "VIEW",     VIEW);
addKeyWord( "INDEX",    INDEX);
addKeyWord( "PROCEDURE",PROCEDURE);
addKeyWord( "TRANSACTION",  TRANSACTION);
addKeyWord( "ISOLATION",    ISOLATION);
addKeyWord( "LEVEL",        LEVEL);
addKeyWord( "READ",         READ);
addKeyWord( "COMMITTED",    COMMITTED);
addKeyWord( "UNCOMMITTED",  UNCOMMITTED);
addKeyWord( "REPEATABLE",   REPEATABLE);
addKeyWord( "SERIALIZABLE", SERIALIZABLE);
addKeyWord( "CONSTRAINT", 	CONSTRAINT);
addKeyWord( "PRIMARY",    	PRIMARY);
addKeyWord( "FOREIGN",     	FOREIGN);
addKeyWord( "KEY",    		KEY);
addKeyWord( "UNIQUE",		UNIQUE);
addKeyWord( "CLUSTERED",  	CLUSTERED);
addKeyWord( "NONCLUSTERED", NONCLUSTERED);
addKeyWord( "REFERENCES",   REFERENCES);
addKeyWord( "UNION", 		UNION);
addKeyWord( "ALL",   		ALL);
addKeyWord( "DISTINCT",   	DISTINCT);
addKeyWord( "CASE",   		CASE);
addKeyWord( "WHEN",   		WHEN);
addKeyWord( "THEN",   		THEN);
addKeyWord( "ELSE",   		ELSE);
addKeyWord( "END",   		END);
addKeyWord( "SWITCH", 		SWITCH);
}
static final int BIT            = 300;
static final int BOOLEAN        = 301;
static final int BINARY         = 310;
static final int VARBINARY      = 311;
static final int RAW      		= 312;
static final int LONGVARBINARY  = 313;
static final int BLOB           = 316;
static final int TINYINT        = 321;
static final int SMALLINT       = 322;
static final int INT            = 323;
static final int COUNTER        = 324; 
static final int BIGINT         = 325;
static final int SMALLMONEY     = 330;
static final int MONEY          = 331;
static final int DECIMAL        = 332;
static final int NUMERIC        = 333;
static final int REAL           = 336;
static final int FLOAT          = 337;
static final int DOUBLE         = 338;
static final int DATE           = 340;
static final int TIME           = 341;
static final int TIMESTAMP      = 342;
static final int SMALLDATETIME  = 343;
static final int CHAR           = 350;
static final int NCHAR          = 352;
static final int VARCHAR        = 353;
static final int NVARCHAR       = 355;
static final int SYSNAME        = 357;
static final int LONGVARCHAR    = 359;
static final int LONGNVARCHAR   = 360;
static final int LONG           = 361;
static final int CLOB           = 362;
static final int NCLOB          = 363;
static final int UNIQUEIDENTIFIER= 370;
static final int JAVA_OBJECT    = 371;
static{
addKeyWord( "BIT",          BIT);
addKeyWord( "BOOLEAN",      BOOLEAN);
addKeyWord( "BINARY",       BINARY);
addKeyWord( "VARBINARY",    VARBINARY);
addKeyWord( "RAW",          RAW); 
addKeyWord( "IMAGE",        LONGVARBINARY); 
addKeyWord( "LONGVARBINARY",LONGVARBINARY);
addKeyWord( "BLOB",         BLOB);
addKeyWord( "BYTE",         TINYINT);
addKeyWord( "TINYINT",      TINYINT);
addKeyWord( "SMALLINT",     SMALLINT);
addKeyWord( "INTEGER",      INT);
addKeyWord( "INT",          INT);
addKeyWord( "SERIAL",       COUNTER); 
addKeyWord( "COUNTER",      COUNTER);
addKeyWord( "BIGINT",       BIGINT);
addKeyWord( "SMALLMONEY",   SMALLMONEY);
addKeyWord( "MONEY",        MONEY);
addKeyWord( "NUMBER",       DECIMAL);
addKeyWord( "VARNUM",       DECIMAL);
addKeyWord( "DECIMAL",      DECIMAL);
addKeyWord( "NUMERIC",      NUMERIC);
addKeyWord( "REAL",         REAL);
addKeyWord( "FLOAT",        FLOAT);
addKeyWord( "DOUBLE",       DOUBLE);
addKeyWord( "DATE",         DATE);
addKeyWord( "TIME",         TIME);
addKeyWord( "DATETIME",     TIMESTAMP); 
addKeyWord( "TIMESTAMP",    TIMESTAMP);
addKeyWord( "SMALLDATETIME",SMALLDATETIME);
addKeyWord( "CHARACTER",    CHAR); 
addKeyWord( "CHAR",         CHAR);
addKeyWord( "NCHAR",        NCHAR);
addKeyWord( "VARCHAR2",     VARCHAR); 
addKeyWord( "VARCHAR",      VARCHAR);
addKeyWord( "NVARCHAR2",    NVARCHAR); 
addKeyWord( "NVARCHAR",     NVARCHAR);
addKeyWord( "SYSNAME",      SYSNAME);
addKeyWord( "TEXT",         LONGVARCHAR);
addKeyWord( "LONGVARCHAR",  LONGVARCHAR);
addKeyWord( "NTEXT",        LONGNVARCHAR);
addKeyWord( "LONGNVARCHAR", LONGNVARCHAR);
addKeyWord( "LONG",         LONG); 
addKeyWord( "CLOB",         CLOB);
addKeyWord( "NCLOB",        NCLOB);
addKeyWord( "UNIQUEIDENTIFIER",UNIQUEIDENTIFIER);
addKeyWord( "SQL_VARIANT",  JAVA_OBJECT); 
addKeyWord( "JAVA_OBJECT",  JAVA_OBJECT);
}
static final int D      = 400;
static final int T      = 401;
static final int TS     = 402;
static final int FN     = 403;
static final int CALL   = 404;
static final int OJ     = 405;
static{
addKeyWord( "D",    D);
addKeyWord( "T",    T);
addKeyWord( "TS",   TS);
addKeyWord( "FN",   FN);
addKeyWord( "CALL", CALL);
addKeyWord( "OJ", OJ);
}
static final int OR     = 500;
static final int AND    = 501;
static final int IS     = 502;
static final int NOT    = 503;
static final int NULL   = 504;
static final int TRUE   = 505;
static final int FALSE  = 506;
static final int BETWEEN= 507;
static final int LIKE   = 508;
static final int IN     = 509;
static{
addKeyWord( "OR",       OR);
addKeyWord( "AND",      AND);
addKeyWord( "IS",       IS);
addKeyWord( "NOT",      NOT);
addKeyWord( "NULL",     NULL);
addKeyWord( "YES",      TRUE); 
addKeyWord( "TRUE",     TRUE);
addKeyWord( "NO",    	FALSE); 
addKeyWord( "FALSE",    FALSE);
addKeyWord( "BETWEEN",  BETWEEN);
addKeyWord( "LIKE",     LIKE);
addKeyWord( "IN",       IN);
}
static final int ABS        = 1000; 
static final int ACOS       = 1001;
static final int ASIN       = 1002;
static final int ATAN       = 1003;
static final int ATAN2      = 1004;
static final int CEILING    = 1005;
static final int COS        = 1006;
static final int COT        = 1007;
static final int DEGREES    = 1008;
static final int EXP        = 1009;
static final int FLOOR      = 1010;
static final int LOG        = 1011;
static final int LOG10      = 1012;
static final int MOD        = 1013;
static final int PI         = 1014;
static final int POWER      = 1015;
static final int RADIANS    = 1016;
static final int RAND       = 1017;
static final int ROUND      = 1018;
static final int SIGN       = 1019;
static final int SIN        = 1020;
static final int SQRT       = 1021;
static final int TAN        = 1022;
static final int TRUNCATE   = 1023; 
static{
addKeyWord( "ABS",      ABS);
addKeyWord( "ACOS",     ACOS);
addKeyWord( "ASIN",     ASIN);
addKeyWord( "ATAN",     ATAN);
addKeyWord( "ATN2",    	ATAN2); 
addKeyWord( "ATAN2",    ATAN2);
addKeyWord( "CEILING",  CEILING);
addKeyWord( "COS",      COS);
addKeyWord( "COT",      COT);
addKeyWord( "DEGREES",  DEGREES);
addKeyWord( "EXP",      EXP);
addKeyWord( "FLOOR",    FLOOR);
addKeyWord( "LOG",      LOG);
addKeyWord( "LOG10",    LOG10);
addKeyWord( "MOD",      MOD);
addKeyWord( "PI",       PI);
addKeyWord( "POWER",    POWER);
addKeyWord( "RADIANS",  RADIANS);
addKeyWord( "RAND",     RAND);
addKeyWord( "ROUND",    ROUND);
addKeyWord( "SIGN",     SIGN);
addKeyWord( "SIN",      SIN);
addKeyWord( "SQRT",     SQRT);
addKeyWord( "TAN",      TAN);
addKeyWord( "TRUNCATE", TRUNCATE);
}
static final int ASCII      = 1100; 
static final int BITLEN     = 1101;
static final int CHARLEN    = 1102;
static final int CHARACTLEN = 1103;
static final int _CHAR      = 1104;
static final int CONCAT     = 1105;
static final int DIFFERENCE = 1106;
static final int INSERT     = 1107;
static final int LCASE      = 1108;
static final int LEFT       = 1109;
static final int LENGTH     = 1110;
static final int LOCATE     = 1111;
static final int LTRIM      = 1112;
static final int OCTETLEN   = 1113;
static final int REPEAT     = 1114;
static final int REPLACE    = 1115;
static final int RIGHT      = 1116;
static final int RTRIM      = 1117;
static final int SOUNDEX    = 1118;
static final int SPACE      = 1119;
static final int SUBSTRING  = 1120;
static final int TRIM       = 1121;
static final int UCASE      = 1122; 
static{
addKeyWord( "ASCII",    ASCII);
addKeyWord( "BIT_LENGTH", BITLEN);
addKeyWord( "CHAR_LENGTH", CHARLEN);
addKeyWord( "CHARACTER_LENGTH", CHARACTLEN);
keywords.put( new Integer(_CHAR), "CHAR" ); 
addKeyWord( "CONCAT",   CONCAT);
addKeyWord( "DIFFERENCE",DIFFERENCE);
addKeyWord( "STUFF",    INSERT); 
addKeyWord( "INSERT",   INSERT);
addKeyWord( "LCASE",    LCASE);
addKeyWord( "LEFT",     LEFT);
addKeyWord( "DATALENGTH",LENGTH); 
addKeyWord( "LEN",		LENGTH); 
addKeyWord( "LENGTH",   LENGTH);
addKeyWord( "CHARINDEX",LOCATE); 
addKeyWord( "LOCATE",   LOCATE);
addKeyWord( "LTRIM",    LTRIM);
addKeyWord( "OCTET_LENGTH", OCTETLEN);
addKeyWord( "REPEAT",   REPEAT);
addKeyWord( "REPLACE",  REPLACE);
addKeyWord( "RIGHT",    RIGHT);
addKeyWord( "RTRIM",    RTRIM);
addKeyWord( "SOUNDEX",  SOUNDEX);
addKeyWord( "SPACE",    SPACE);
addKeyWord( "SUBSTRING",SUBSTRING);
addKeyWord( "TRIM",     TRIM);
addKeyWord( "UCASE",    UCASE);
}
static final int CURDATE    = 1200; 
static final int CURRENTDATE = 1201;
static final int CURTIME    = 1202;
static final int DAYNAME    = 1203;
static final int DAYOFMONTH = 1204;
static final int DAYOFWEEK  = 1205;
static final int DAYOFYEAR  = 1206;
static final int DAY		= 1207;
static final int HOUR       = 1208;
static final int MILLISECOND= 1209;
static final int MINUTE     = 1210;
static final int MONTH      = 1211;
static final int MONTHNAME  = 1212;
static final int NOW        = 1213;
static final int QUARTER    = 1214;
static final int SECOND     = 1215;
static final int TIMESTAMPADD=1216;
static final int TIMESTAMPDIFF=1217;
static final int WEEK       = 1218;
static final int YEAR       = 1219; 
static{
addKeyWord( "CURDATE",      CURDATE);
addKeyWord( "CURTIME",      CURTIME);
addKeyWord( "CURRENT_DATE", CURRENTDATE);
addKeyWord( "DAYNAME",      DAYNAME);
addKeyWord( "DAYOFMONTH",   DAYOFMONTH);
addKeyWord( "DAYOFWEEK",    DAYOFWEEK);
addKeyWord( "DAYOFYEAR",    DAYOFYEAR);
addKeyWord( "DAY",    		DAY);
addKeyWord( "HOUR",         HOUR);
addKeyWord( "MILLISECOND",  MILLISECOND);
addKeyWord( "MINUTE",       MINUTE);
addKeyWord( "MONTH",        MONTH);
addKeyWord( "MONTHNAME",    MONTHNAME);
addKeyWord( "GETDATE",      NOW); 
addKeyWord( "NOW",          NOW);
addKeyWord( "QUARTER",      QUARTER);
addKeyWord( "SECOND",       SECOND);
addKeyWord( "DATEADD", 		TIMESTAMPADD); 
addKeyWord( "TIMESTAMPADD", TIMESTAMPADD);
addKeyWord( "DATEDIFF",		TIMESTAMPDIFF); 
addKeyWord( "TIMESTAMPDIFF",TIMESTAMPDIFF);
addKeyWord( "WEEK",         WEEK);
addKeyWord( "YEAR",         YEAR);
}
static final int SQL_TSI_FRAC_SECOND= 1250;
static final int SQL_TSI_SECOND		= 1251;
static final int SQL_TSI_MINUTE		= 1252;
static final int SQL_TSI_HOUR		= 1253;
static final int SQL_TSI_DAY		= 1254;
static final int SQL_TSI_WEEK		= 1255;
static final int SQL_TSI_MONTH		= 1256;
static final int SQL_TSI_QUARTER	= 1257;
static final int SQL_TSI_YEAR		= 1258;
static{
addKeyWord( "MS",					SQL_TSI_FRAC_SECOND);
addKeyWord( "SQL_TSI_FRAC_SECOND",	SQL_TSI_FRAC_SECOND);
addKeyWord( "S",					SQL_TSI_SECOND);
addKeyWord( "SS",					SQL_TSI_SECOND);
addKeyWord( "SQL_TSI_SECOND",		SQL_TSI_SECOND);
addKeyWord( "MI",					SQL_TSI_MINUTE);
addKeyWord( "N",					SQL_TSI_MINUTE);
addKeyWord( "SQL_TSI_MINUTE",		SQL_TSI_MINUTE);
addKeyWord( "HH",					SQL_TSI_HOUR);
addKeyWord( "SQL_TSI_HOUR",			SQL_TSI_HOUR);
addKeyWord( "DD",					SQL_TSI_DAY);
addKeyWord( "SQL_TSI_DAY",			SQL_TSI_DAY);
addKeyWord( "WK",					SQL_TSI_WEEK);
addKeyWord( "WW",					SQL_TSI_WEEK);
addKeyWord( "SQL_TSI_WEEK",			SQL_TSI_WEEK);
addKeyWord( "M",					SQL_TSI_MONTH);
addKeyWord( "MM",					SQL_TSI_MONTH);
addKeyWord( "SQL_TSI_MONTH",		SQL_TSI_MONTH);
addKeyWord( "Q",					SQL_TSI_QUARTER);
addKeyWord( "QQ",					SQL_TSI_QUARTER);
addKeyWord( "SQL_TSI_QUARTER",		SQL_TSI_QUARTER);
addKeyWord( "YY",					SQL_TSI_YEAR);
addKeyWord( "YYYY",					SQL_TSI_YEAR);
addKeyWord( "SQL_TSI_YEAR",			SQL_TSI_YEAR);
}
static final int IFNULL     = 1301; 
static final int USER       = 1302;
static final int CONVERT    = 1303;
static final int CAST    	= 1304;
static final int IIF    	= 1305; 
static{
addKeyWord( "ISNULL",      	IFNULL); 
addKeyWord( "IFNULL",       IFNULL);
addKeyWord( "USER",         USER);
addKeyWord( "CONVERT",      CONVERT);
addKeyWord( "CAST",      	CAST);
addKeyWord( "IIF",      	IIF);
}
static final int SQL_BIGINT    		= 1350;
static final int SQL_BINARY    		= 1351;
static final int SQL_BIT    		= 1352;
static final int SQL_CHAR    		= 1353;
static final int SQL_DATE    		= 1354;
static final int SQL_DECIMAL    	= 1355;
static final int SQL_DOUBLE    		= 1356;
static final int SQL_FLOAT    		= 1357;
static final int SQL_INTEGER    	= 1358;
static final int SQL_LONGVARBINARY 	= 1359;
static final int SQL_LONGVARCHAR 	= 1360;
static final int SQL_REAL    		= 1361;
static final int SQL_SMALLINT    	= 1362;
static final int SQL_TIME    		= 1363;
static final int SQL_TIMESTAMP    	= 1364;
static final int SQL_TINYINT    	= 1365;
static final int SQL_VARBINARY    	= 1366;
static final int SQL_VARCHAR    	= 1367;
static{
addKeyWord( "SQL_BIGINT",		SQL_BIGINT);
addKeyWord( "SQL_BINARY",		SQL_BINARY);
addKeyWord( "SQL_BIT",			SQL_BIT);
addKeyWord( "SQL_CHAR",			SQL_CHAR);
addKeyWord( "SQL_DATE",			SQL_DATE);
addKeyWord( "SQL_DECIMAL",		SQL_DECIMAL);
addKeyWord( "SQL_DOUBLE",		SQL_DOUBLE);
addKeyWord( "SQL_FLOAT",		SQL_FLOAT);
addKeyWord( "SQL_INTEGER",		SQL_INTEGER);
addKeyWord( "SQL_LONGVARBINARY",SQL_LONGVARBINARY);
addKeyWord( "SQL_LONGVARCHAR",	SQL_LONGVARCHAR);
addKeyWord( "SQL_REAL",			SQL_REAL);
addKeyWord( "SQL_SMALLINT",		SQL_SMALLINT);
addKeyWord( "SQL_TIME",			SQL_TIME);
addKeyWord( "SQL_TIMESTAMP",	SQL_TIMESTAMP);
addKeyWord( "SQL_TINYINT",		SQL_TINYINT);
addKeyWord( "SQL_VARBINARY",	SQL_VARBINARY);
addKeyWord( "SQL_VARCHAR",		SQL_VARCHAR);
}
static final int COUNT		= 1400;
static final int MIN		= 1401;
static final int MAX		= 1402;
static final int SUM		= 1403;
static final int FIRST		= 1404;
static final int LAST		= 1405;
static final int AVG		= 1406;
static{
addKeyWord( "COUNT",       	COUNT);
addKeyWord( "MIN",      	MIN);
addKeyWord( "MAX",      	MAX);
addKeyWord( "SUM",         	SUM);
addKeyWord( "FIRST",        FIRST);
addKeyWord( "LAST",         LAST);
addKeyWord( "AVG",          AVG);
}
}
class SearchNode{
int value;
char letter;
SearchNode nextLetter; 
SearchNode nextEntry;  
}
package smallsql.database;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
final class TableViewMap{
private final HashMap map = new HashMap();
private Object getUniqueKey(String name){
return name.toUpperCase(Locale.US); 
}
TableView get(String name){
return (TableView)map.get(getUniqueKey(name));
}
void put(String name, TableView tableView){
map.put(getUniqueKey(name), tableView);
}
TableView remove(String name){
return (TableView)map.remove(getUniqueKey(name));
}
Collection values(){
return map.values();
}
}
package smallsql.database;
final class Columns {
private int size;
private Column[] data;
Columns(){
data = new Column[16];
}
final int size(){
return size;
}
final Column get(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
return data[idx];
}
final Column get(String name){
for(int i = 0; i < size; i++){
Column column = data[i];
if(name.equalsIgnoreCase(column.getName())){
return column;
}
}
return null;
}
final void add(Column column){
if(column == null){
throw new NullPointerException("Column is null.");
}
if(size >= data.length){
resize(size << 1);
}
data[size++] = column;
}
Columns copy(){
Columns copy = new Columns();
Column[] cols = copy.data = (Column[]) data.clone();
for(int i=0; i<size; i++){
cols[i] = cols[i].copy();
}
copy.size = size;
return copy;
}
private final void resize(int newSize){
Column[] dataNew = new Column[newSize];
System.arraycopy(data, 0, dataNew, 0, size);
data = dataNew;
}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class GroupResult extends MemoryResult{
private Expression currentGroup; 
private RowSource from;
private Expressions groupBy; 
private Expressions expressions = new Expressions(); 
private Expressions internalExpressions = new Expressions(); 
GroupResult(CommandSelect cmd, RowSource from, Expressions groupBy, Expression having, Expressions orderBy) throws SQLException{
this.from = from;
this.groupBy = groupBy;
if(groupBy != null){
for(int i=0; i<groupBy.size(); i++){
Expression left = groupBy.get(i);
int idx = addInternalExpressionFromGroupBy( left );
ExpressionName right = new ExpressionName(null);
right.setFrom(this, idx, new ColumnExpression(left));
Expression expr = new ExpressionArithmetic( left, right, ExpressionArithmetic.EQUALS_NULL);
currentGroup = (currentGroup == null) ?
expr :
new ExpressionArithmetic( currentGroup, expr, ExpressionArithmetic.AND );
}
}
expressions = internalExpressions;
for(int c=0; c<expressions.size(); c++){
addColumn(new ColumnExpression(expressions.get(c)));
}
patchExpressions( cmd.columnExpressions );
if(having != null) having = patchExpression( having );
patchExpressions( orderBy );
}
final private int addInternalExpressionFromGroupBy(Expression expr) throws SQLException{
int type = expr.getType();
if(type >= Expression.GROUP_BEGIN){
throw SmallSQLException.create(Language.GROUP_AGGR_INVALID, expr);
}else{
int idx = internalExpressions.indexOf(expr);
if(idx >= 0) return idx;
internalExpressions.add(expr);
return internalExpressions.size()-1;
}
}
final private int addInternalExpressionFromSelect(Expression expr) throws SQLException{
int type = expr.getType();
if(type == Expression.NAME){
int idx = internalExpressions.indexOf(expr);
if(idx >= 0) return idx;
throw SmallSQLException.create(Language.GROUP_AGGR_NOTPART, expr);
}else
if(type >= Expression.GROUP_BEGIN){
int idx = internalExpressions.indexOf(expr);
if(idx >= 0) return idx;
internalExpressions.add(expr);
return internalExpressions.size()-1;
}else{
int idx = internalExpressions.indexOf(expr);
if(idx >= 0) return idx;
Expression[] params = expr.getParams();
if(params != null){
for(int p=0; p<params.length; p++){
addInternalExpressionFromSelect( params[p]);
}
}
return -1;
}
}
final private void patchExpressions(Expressions exprs) throws SQLException{
if(exprs == null) return;
for(int i=0; i<exprs.size(); i++){
exprs.set(i, patchExpression(exprs.get(i)));
}
}
final private void patchExpressions(Expression expression) throws SQLException{
Expression[] params = expression.getParams();
if(params == null) return;
for(int i=0; i<params.length; i++){
expression.setParamAt( patchExpression(params[i]), i);
}
}
final private Expression patchExpression(Expression expr) throws SQLException{
int idx = addInternalExpressionFromSelect( expr );
if(idx>=0){
Expression origExpression = expr;
ExpressionName exprName;
if(expr instanceof ExpressionName){
exprName = (ExpressionName)expr;
}else{
expr = exprName = new ExpressionName(expr.getAlias());
}
Column column = exprName.getColumn();
if(column == null){
column = new Column();
exprName.setFrom(this, idx, column);
switch(exprName.getType()){
case Expression.MAX:
case Expression.MIN:
case Expression.FIRST:
case Expression.LAST:
case Expression.SUM:
Expression baseExpression = exprName.getParams()[0];
column.setPrecision(baseExpression.getPrecision());
column.setScale(baseExpression.getScale());
break;
default:
column.setPrecision(origExpression.getPrecision());
column.setScale(origExpression.getScale());
}
column.setDataType(exprName.getDataType());
}else{
exprName.setFrom(this, idx, column);
}
}else{
patchExpressions(expr);
}
return expr;
}
final void execute() throws Exception{
super.execute();
from.execute();
NextRow:
while(from.next()){
beforeFirst();
while(next()){
if(currentGroup == null || currentGroup.getBoolean()){
accumulateRow();
continue NextRow;
}
}
addGroupRow();
accumulateRow();
}
if(getRowCount() == 0 && groupBy == null){
addGroupRow();
}
beforeFirst();
}
final private void addGroupRow(){
ExpressionValue[] newRow = currentRow = new ExpressionValue[ expressions.size()];
for(int i=0; i<newRow.length; i++){
Expression expr = expressions.get(i);
int type = expr.getType();
if(type < Expression.GROUP_BEGIN) type = Expression.GROUP_BY;
newRow[i] = new ExpressionValue( type );
}
addRow(newRow);
}
final private void accumulateRow() throws Exception{
for(int i=0; i<currentRow.length; i++){
Expression src = expressions.get(i);
currentRow[i].accumulate(src);
}
}
}
package smallsql.database;
final  class NoFromResult extends RowSource {
private int rowPos; 
final boolean isScrollable(){
return true;
}
final void beforeFirst(){
rowPos = 0;
}
final boolean isBeforeFirst(){
return rowPos <= 0;
}
final boolean isFirst(){
return rowPos == 1;
}
final boolean first(){
rowPos = 1;
return true;
}
final boolean previous(){
rowPos--;
return rowPos == 1;
}
final boolean next(){
rowPos++;
return rowPos == 1;
}
final boolean last(){
rowPos = 1;
return true;
}
final boolean isLast(){
return rowPos == 1;
}
final boolean isAfterLast(){
return rowPos > 1;
}
final void afterLast(){
rowPos = 2;
}
final boolean absolute(int row){
rowPos = (row > 0) ?
Math.min( row, 1 ) :
Math.min( row +1, -1 );
return rowPos == 1;
}
final boolean relative(int rows){
if(rows == 0) return rowPos == 1;
rowPos = Math.min( Math.max( rowPos + rows, -1), 1);
return rowPos == 1;
}
final int getRow(){
return rowPos == 1 ? 1 : 0;
}
final long getRowPosition() {
return rowPos;
}
final void setRowPosition(long rowPosition){
rowPos = (int)rowPosition;
}
final boolean rowInserted(){
return false;
}
final boolean rowDeleted(){
return false;
}
final void nullRow() {
throw new Error();
}
final void noRow() {
throw new Error();
}
final void execute() throws Exception{
boolean isExpressionsFromThisRowSource(Expressions columns){
return columns.size() == 0;
}
}
package smallsql.database;
import java.sql.*;
import java.util.Properties;
import java.util.StringTokenizer;
import smallsql.database.language.Language;
public class SSDriver implements Driver {
static final String URL_PREFIX = "jdbc:smallsql";
static SSDriver drv;
static {
try{
drv = new SSDriver();
java.sql.DriverManager.registerDriver(drv);
}catch(Throwable e){
e.printStackTrace();
}
}
public Connection connect(String url, Properties info) throws SQLException{
if(!acceptsURL(url)){
return null;
}
return new SSConnection(parse(url, info));
}
private Properties parse(String url, Properties info) throws SQLException {
Properties props = (Properties)info.clone();
if(!acceptsURL(url)){
return props;
}
int idx1 = url.indexOf(':', 5); 
int idx2 = url.indexOf('?');
if(idx1 > 0){
String dbPath = (idx2 > 0) ? url.substring(idx1 + 1, idx2) : url.substring(idx1 + 1);
props.setProperty("dbpath", dbPath);
}
if(idx2 > 0){
String propsString = url.substring(idx2 + 1).replace('&', ';');
StringTokenizer tok = new StringTokenizer(propsString, ";");
while(tok.hasMoreTokens()){
String keyValue = tok.nextToken().trim();
if(keyValue.length() > 0){
idx1 = keyValue.indexOf('=');
if(idx1 > 0){
String key = keyValue.substring(0, idx1).toLowerCase().trim();
String value = keyValue.substring(idx1 + 1).trim();
props.put(key, value);
}else{
throw SmallSQLException.create(Language.CUSTOM_MESSAGE, "Missing equal in property:" + keyValue);
}
}
}
}
return props;
}
public boolean acceptsURL(String url){
return url.startsWith(URL_PREFIX);
}
public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
throws SQLException {
Properties props = parse(url, info);
DriverPropertyInfo[] driverInfos = new DriverPropertyInfo[1];
driverInfos[0] = new DriverPropertyInfo("dbpath", props.getProperty("dbpath"));
return driverInfos;
}
public int getMajorVersion() {
return 0;
}
public int getMinorVersion() {
return 21;
}
public boolean jdbcCompliant() {
return true;
}
}
package smallsql.junit;
import junit.framework.*;
import java.sql.*;
import java.math.*;
public class TestDataTypes extends BasicTestCase{
static final String[] DATATYPES = { "varchar(100)",
"varchar2(130)", "nvarchar(137)", "nvarchar2(137)", "sysname",
"char(100)", "CHARACTER(99)",
"nchar(80)",
"int", "smallint", "tinyint", "bigint", "byte",
"real", "float", "double",
"bit", "Boolean",
"binary( 125 )", "varbinary(57)", "raw(88)",
"java_object", "sql_variant",
"image", "LONGvarbinary", "long raw",
"blob", "clob","nclob",
"text", "ntext", "LongVarchar", "long",
"time", "date", "datetime", "timestamp", "SMALLDATETIME",
"UNIQUEIDENTIFIER",
"numeric(28,4)", "decimal(29,4)","number(29,4)", "varnum(29,4)",
"COUNTER",
"money", "smallmoney"};
private static final String table = "table_datatypes";
private String datatype;
TestDataTypes( String datatype ){
super( datatype );
this.datatype = datatype;
}
public void tearDown(){
try{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("drop table " + table);
st.close();
}catch(Throwable e){
}
}
public void setUp(){
tearDown();
}
public void runTest() throws Throwable {
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("Create Table " + table +"(abc " + datatype + ")");
String name = "abc";
Object[] values = null;
String   quote = "";
String escape1 = "";
String escape2 = "";
boolean needTrim = false;
ResultSet rs = st.executeQuery("SELECT * From " + table);
ResultSetMetaData md = rs.getMetaData();
switch(md.getColumnType(1)){
case Types.CHAR:
needTrim = true;
case Types.VARCHAR:
case Types.LONGVARCHAR:
case Types.CLOB:
values = new Object[]{null,"qwert", "asdfg", "hjhjhj", "1234567890 qwertzuiop 1234567890 asdfghjklö 1234567890 yxcvbnm,.- 1234567890 "};
quote  = "\'";
break;
case Types.BIGINT:
values = new Object[]{null,new Long(123), new Long(-2123), new Long(392839283)};
break;
case Types.INTEGER:
values = new Object[]{null,new Integer(123), new Integer(-2123), new Integer(392839283)};
break;
case Types.SMALLINT:
values = new Object[]{null,new Integer(123), new Integer(-2123), new Integer(32000)};
break;
case Types.TINYINT:
values = new Object[]{null,new Integer(0), new Integer(12), new Integer(228)};
break;
case Types.REAL:
values = new Object[]{null,new Float(0.0), new Float(-12.123), new Float(22812345234.9)};
break;
case Types.FLOAT:
case Types.DOUBLE:
values = new Object[]{null,new Double(0.0), new Double(-12.123), new Double(22812345234.9)};
break;
case Types.NUMERIC:
case Types.DECIMAL:
needTrim = true;
if(md.getPrecision(1)<16){
values = new Object[]{null,new BigDecimal("0.0"), new BigDecimal("-2"), new BigDecimal("-12.123")};
}else{
values = new Object[]{null,new BigDecimal("0.0"), new BigDecimal("-2"), new BigDecimal("-12.123"), new BigDecimal("22812345234.9")};
}
break;
case Types.BIT:
case Types.BOOLEAN:
values = new Object[]{null, Boolean.TRUE, Boolean.FALSE};
break;
case Types.TIME:
values = new Object[]{null, new Time(10,17,56), new Time(0,0,0),new Time(23,59,59)};
escape1 = "{t '";
escape2 = "'}";
break;
case Types.DATE:
values = new Object[]{null, new java.sql.Date(10,10,1), new java.sql.Date(0,0,1),new java.sql.Date(70,0,1)};
escape1 = "{d '";
escape2 = "'}";
break;
case Types.TIMESTAMP:
if(md.getPrecision(1) >16)
values = new Object[]{null, new Timestamp(10,10,1, 10,17,56, 0), new Timestamp(0,0,1, 0,0,0, 0),new Timestamp( 120,1,1, 23,59,59, 500000000),new Timestamp(0),new Timestamp( -120,1,1, 23,59,59, 500000000)};
else
values = new Object[]{null, new Timestamp(10,10,1, 10,17,0, 0), new Timestamp(0,0,1, 0,0,0, 0),new Timestamp(0)};
escape1 = "{ts '";
escape2 = "'}";
break;
case Types.BINARY:
needTrim = true;
case Types.VARBINARY:
case Types.LONGVARBINARY:
case Types.BLOB:
values = new Object[]{null, new byte[]{1, 127, -23}};
break;
case Types.JAVA_OBJECT:
values = new Object[]{null, new Integer(-123), new Double(1.2), new byte[]{1, 127, -23}};
break;
case -11: 
values = new Object[]{null, "342734E3-D9AC-408F-8724-B7A257C4529E", "342734E3-D9AC-408F-8724-B7A257C4529E"};
quote  = "\'";
break;
default: fail("Unknown column type: " + rs.getMetaData().getColumnType(1));
}
rs.close();
con.close();
con = AllTests.getConnection();
st = con.createStatement();
for(int i=0; i<values.length; i++){
Object val = values[i];
String q = (val == null) ? "" : quote;
String e1 = (val == null) ? "" : escape1;
String e2 = (val == null) ? "" : escape2;
if(val instanceof byte[]){
StringBuffer buf = new StringBuffer( "0x" );
for(int k=0; k<((byte[])val).length; k++){
String digit = "0" + Integer.toHexString( ((byte[])val)[k] );
buf.append( digit.substring( digit.length()-2 ) );
}
val = buf.toString();
}
st.execute("Insert into " + table + "(abc) Values(" + e1 + q + val + q + e2 + ")");
}
checkValues( st, values, needTrim);
st.execute("Delete From "+ table);
CallableStatement cal = con.prepareCall("Insert Into " + table + "(abc) Values(?)");
for(int i=0; i<values.length; i++){
Object val = values[i];
cal.setObject( 1, val);
cal.execute();
}
cal.close();
checkValues( st, values, needTrim);
st.execute("Delete From "+ table);
cal = con.prepareCall("Insert Into " + table + "(abc) Values(?)");
for(int i=0; i<values.length; i++){
Object val = values[i];
if(val == null){
cal.setNull( 1, Types.NULL );
}else
if(val instanceof Time){
cal.setTime( 1, (Time)val );
}else
if(val instanceof Timestamp){
cal.setTimestamp( 1, (Timestamp)val );
}else
if(val instanceof Date){
cal.setDate( 1, (Date)val );
}else
if(val instanceof String){
cal.setString( 1, (String)val );
}else
if(val instanceof Boolean){
cal.setBoolean( 1, ((Boolean)val).booleanValue() );
}else
if(val instanceof Byte){
cal.setByte( 1, ((Byte)val).byteValue() );
}else
if(val instanceof Short){
cal.setShort( 1, ((Short)val).shortValue() );
}else
if(val instanceof Integer){
cal.setInt( 1, ((Integer)val).intValue() );
}else
if(val instanceof Long){
cal.setLong( 1, ((Long)val).longValue() );
}else
if(val instanceof Float){
cal.setFloat( 1, ((Float)val).floatValue() );
}else
if(val instanceof Double){
cal.setDouble( 1, ((Double)val).doubleValue() );
}else
if(val instanceof BigDecimal){
cal.setBigDecimal( 1, (BigDecimal)val );
}else
if(val instanceof byte[]){
cal.setBytes( 1, (byte[])val );
}
cal.execute();
}
cal.close();
checkValues( st, values, needTrim);
st.execute("Delete From "+ table);
Statement st2 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
ResultSet rs2 = st2.executeQuery("SELECT * From " + table);
for(int i=0; i<values.length; i++){
rs2.moveToInsertRow();
Object val = values[i];
if(val == null){
rs2.updateNull( name );
}else
if(val instanceof Time){
rs2.updateTime( name, (Time)val );
}else
if(val instanceof Timestamp){
rs2.updateTimestamp( name, (Timestamp)val );
}else
if(val instanceof Date){
rs2.updateDate( name, (Date)val );
}else
if(val instanceof String){
rs2.updateString( name, (String)val );
}else
if(val instanceof Boolean){
rs2.updateBoolean( name, ((Boolean)val).booleanValue() );
}else
if(val instanceof Byte){
rs2.updateByte( name, ((Byte)val).byteValue() );
}else
if(val instanceof Short){
rs2.updateShort( name, ((Short)val).shortValue() );
}else
if(val instanceof Integer){
rs2.updateInt( name, ((Integer)val).intValue() );
}else
if(val instanceof Long){
rs2.updateLong( name, ((Long)val).longValue() );
}else
if(val instanceof Float){
rs2.updateFloat( name, ((Float)val).floatValue() );
}else
if(val instanceof Double){
rs2.updateDouble( name, ((Double)val).doubleValue() );
}else
if(val instanceof BigDecimal){
rs2.updateBigDecimal( name, (BigDecimal)val );
}else
if(val instanceof byte[]){
rs2.updateBytes( name, (byte[])val );
}
rs2.insertRow();
}
st2.close();
checkValues( st, values, needTrim);
}
private void checkValues(Statement st, Object[] values, boolean needTrim) throws Exception{
ResultSet rs = st.executeQuery("SELECT * From " + table);
int i = 0;
while(rs.next()){
assertEqualsRsValue(values[i], rs, needTrim);
i++;
}
rs.close();
}
public static Test suite() throws Exception{
TestSuite theSuite = new TestSuite("Data Types");
for(int i=0; i<DATATYPES.length; i++){
theSuite.addTest(new TestDataTypes( DATATYPES[i] ) );
}
return theSuite;
}
public static void main(String[] argv) {
junit.swingui.TestRunner.main(new String[]{TestDataTypes.class.getName()});
}
}
package smallsql.junit;
import java.sql.*;
public class TestTransactions extends BasicTestCase {
public void testCreateTable() throws Exception{
Connection con = AllTests.getConnection();
Connection con2 = AllTests.createConnection();
try{
con.setAutoCommit(false);
con.createStatement().execute("create table transactions (ID  INTEGER NOT NULL, Name VARCHAR(100), FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
con.commit();
con2.setAutoCommit(false);
PreparedStatement pr = con2.prepareStatement("insert into transactions (id,Name,FirstName,Points,LicenseID) values (?,?,?,?,?)");
pr.setInt( 		1, 0 );
pr.setString( 	2, "Pilot_1" );
pr.setString( 	3, "Herkules" );
pr.setInt( 		4, 1 );
pr.setInt( 		5, 1 );
pr.addBatch();
pr.executeBatch();
assertRowCount( 0, "Select * from transactions");
con2.commit();
assertRowCount( 1, "Select * from transactions");
}finally{
con2.close();
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
public void testCommit() throws Exception{
Connection con = AllTests.getConnection();
try{
con.setAutoCommit(false);
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
assertRowCount( 1, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
assertRowCount( 2, "Select * from transactions");
con.createStatement().execute("Insert Into transactions Select * From transactions");
assertRowCount( 4, "Select * from transactions");
con.commit();
assertRowCount( 4, "Select * from transactions");
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
public void testCommitWithOneCommitRow() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
assertRowCount( 1, "Select * from transactions");
con.setAutoCommit(false);
con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
assertRowCount( 2, "Select * from transactions");
con.createStatement().execute("Insert Into transactions (Select * From transactions)");
assertRowCount( 4, "Select * from transactions");
con.commit();
assertRowCount( 4, "Select * from transactions");
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
public void testRollback() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
con.setAutoCommit(false);
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
assertRowCount( 1, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
assertRowCount( 2, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) (Select v From transactions)");
assertRowCount( 4, "Select * from transactions");
con.rollback();
assertRowCount( 0, "Select * from transactions");
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
public void testRollbackWithOneCommitRow() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
assertRowCount( 1, "Select * from transactions");
con.setAutoCommit(false);
con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
assertRowCount( 2, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) (Select v From transactions)");
assertRowCount( 4, "Select * from transactions");
con.rollback();
assertRowCount( 1, "Select * from transactions");
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
private void testInsertRow_Last(Connection con, boolean callLastBefore) throws Exception{
try{
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
ResultSet rs = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
.executeQuery("Select * from transactions Where 1=0");
if(callLastBefore) rs.last();
rs.moveToInsertRow();
rs.updateString("v", "qwert2");
rs.insertRow();
rs.last();
assertEquals("qwert2", rs.getString("v"));
assertFalse( rs.next() );
assertTrue( rs.previous() );
assertEquals("qwert2", rs.getString("v"));
rs.beforeFirst();
assertTrue( rs.next() );
assertEquals("qwert2", rs.getString("v"));
assertFalse( rs.next() );
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
}
}
public void testInsertRow_Last() throws Exception{
Connection con = AllTests.getConnection();
testInsertRow_Last(con, false);
testInsertRow_Last(con, true);
con.setAutoCommit(false);
testInsertRow_Last(con, false);
con.setAutoCommit(true);
con.setAutoCommit(false);
testInsertRow_Last(con, true);
con.setAutoCommit(true);
}
public void testInsertAndUpdate() throws Exception{
Connection con = AllTests.getConnection();
try{
con.setAutoCommit(false);
con.createStatement().execute("Create Table transactions ( v varchar(20))");
assertRowCount( 0, "Select * from transactions");
assertEquals( 1, con.createStatement().executeUpdate("Insert Into transactions(v) Values('qwert')") );
assertEqualsRsValue("qwert", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
assertEquals( 1, con.createStatement().executeUpdate("Update transactions set v='qwert1'") );
assertEqualsRsValue("qwert1", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
assertEquals( 1, con.createStatement().executeUpdate("Update transactions set v='qwert2'") );
assertEqualsRsValue("qwert2", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
Savepoint savepoint = con.setSavepoint();
assertEquals( 1, con.createStatement().executeUpdate("Update transactions set v='qwert 3'") );
assertEqualsRsValue("qwert 3", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
con.rollback( savepoint );
con.commit();
assertEqualsRsValue("qwert2", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
public void testUpdateAndSavepoint() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table transactions ( v varchar(20))");
assertRowCount(0, "Select * from transactions");
assertEquals(1, con.createStatement().executeUpdate("Insert Into transactions(v) Values('qwert')"));
assertEqualsRsValue("qwert", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
con.setAutoCommit(false);
assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert1'"));
assertEqualsRsValue("qwert1", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert2'"));
assertEqualsRsValue("qwert2", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
Savepoint savepoint = con.setSavepoint();
assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert 3'"));
assertEqualsRsValue("qwert 3", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert 4'"));
assertEqualsRsValue("qwert 4", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert 5'"));
assertEqualsRsValue("qwert 5", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
con.rollback(savepoint);
con.commit();
assertEqualsRsValue("qwert2", "Select * from transactions");
assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
}finally{
dropTable(con, "transactions");
con.setAutoCommit(true);
}
}
public void testInsertRow_withWrongWhere() throws Exception{
Connection con = AllTests.getConnection();
try{
con.setAutoCommit(false);
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
ResultSet rs = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
.executeQuery("Select * from transactions Where 1=0");
rs.moveToInsertRow();
rs.updateString("v", "qwert2");
rs.insertRow();
rs.beforeFirst();
assertTrue( rs.next() );
assertEquals("qwert2", rs.getString("v"));
assertFalse( rs.next() );
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
con.setAutoCommit(true);
}
}
public void testInsertRow_withRightWhere() throws Exception{
Connection con = AllTests.getConnection();
try{
con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con.createStatement().execute("Insert Into transactions(v) Values('qwert2')");
ResultSet rs = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
.executeQuery("Select * from transactions Where v = 'qwert'");
rs.moveToInsertRow();
rs.updateString("v", "qwert");
rs.insertRow();
rs.beforeFirst();
assertTrue( rs.next() );
assertEquals("qwert", rs.getString("v"));
assertFalse( rs.next() );
}finally{
try{
con.createStatement().execute("Drop Table transactions");
}catch(Throwable e){e.printStackTrace();}
}
}
public void testReadUncommited() throws Exception{
Connection con1 = AllTests.getConnection();
Connection con2 = AllTests.createConnection();
try{
con2.setTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con1.setAutoCommit(false);
con1.createStatement().execute("Insert Into transactions(v) Values('qwert2')");
ResultSet rs2 = con2.createStatement().executeQuery("Select count(*) from transactions");
assertTrue( rs2.next() );
assertEquals( 1, rs2.getInt(1) );
}finally{
dropTable(con1, "transactions");
con1.setAutoCommit(true);
con2.close();
}
}
public void testReadCommited() throws Exception{
Connection con1 = AllTests.getConnection();
Connection con2 = AllTests.createConnection();
try{
con2.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con1.setAutoCommit(false);
con1.createStatement().execute("Insert Into transactions(v) Values('qwert2')");
ResultSet rs2 = con2.createStatement().executeQuery("Select count(*) from transactions");
assertTrue( rs2.next() );
assertEquals( 0, rs2.getInt(1) );
}finally{
dropTable(con1, "transactions");
con1.setAutoCommit(true);
con2.close();
}
}
public void testReadSerialized() throws Exception{
Connection con1 = AllTests.getConnection();
Connection con2 = AllTests.createConnection();
try{
con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
assertRowCount( 0, "Select * from transactions");
con1.createStatement().execute("Insert Into transactions(v) Values('qwert2')");
assertRowCount( 1, "Select * from transactions");
con1.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
con1.setAutoCommit(false);
ResultSet rs1 = con1.createStatement().executeQuery("Select count(*) from transactions");
assertTrue( rs1.next() );
assertEquals( "Count(*)", 1, rs1.getInt(1) );
ResultSet rs2 = con2.createStatement().executeQuery("Select count(*) from transactions");
assertTrue( rs2.next() );
assertEquals( "Count(*)", 1, rs2.getInt(1) );
try{
con2.createStatement().execute("Insert Into transactions(v) Values('qwert3')");
fail("TRANSACTION_SERIALIZABLE does not lock the table");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
}finally{
con2.close();
dropTable(con1, "transactions");
con1.setAutoCommit(true);
}
}
public void testReadWriteLock() throws Exception{
Connection con1 = AllTests.getConnection();
Connection con2 = AllTests.createConnection();
try{
con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
con1.createStatement().execute("Insert Into transactions(v) Values('qwert1')");
con1.setAutoCommit(false);
con1.createStatement().execute("Update transactions Set v = 'qwert'");
long time = System.currentTimeMillis();
try{
con2.createStatement().executeQuery("Select count(*) from transactions");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
assertTrue("Wait time to small", System.currentTimeMillis()-time>=5000);
}finally{
con2.close();
con1.setAutoCommit(true);
dropTable(con1, "transactions");
}
}
}
package smallsql.database;
class ExpressionFunctionFloor extends ExpressionFunctionReturnP1Number {
int getFunction(){ return SQLTokenizer.FLOOR; }
double getDouble() throws Exception{
return Math.floor( param1.getDouble() );
}
String getString() throws Exception{
Object obj = getObject();
if(obj == null) return null;
return obj.toString();
}
}
package smallsql.junit;
import java.sql.*;
public class TestStatement extends BasicTestCase {
private static boolean init;
protected void setUp() throws Exception{
if(init) return;
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
dropTable( con, "statement");
st.execute("Create Table statement (c varchar(30), i counter)");
init = true;
}
public void testBatchUpate() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
assertEquals("Result Length wrong", 0, st.executeBatch().length );
st.clearBatch();
st.addBatch("Bla Bla");
try {
st.executeBatch();
} catch (BatchUpdateException ex) {
assertEquals("Result Length wrong",1,ex.getUpdateCounts().length);
}
st.clearBatch();
int count = 10;
for(int i=1; i<=count; i++){
st.addBatch("Insert Into statement(c) Values('batch"+i+"')");
}
int[] result = st.executeBatch();
assertEquals("Result Length wrong", count, result.length);
for(int i=0; i<count; i++){
assertEquals("Update Count", 1, result[i]);
}
assertRowCount(10, "Select * From statement");
}
public void testMultiValues() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
assertEquals("Update Count:", 10, st.executeUpdate("Insert Into statement(c) Values('abc1'),('abc2'),('abc3'),('abc4'),('abc5'),('abc6'),('abc7'),('abc8'),('abc9'),('abc10')"));
}
public void testMaxRows() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.setMaxRows(5);
ResultSet rs = st.executeQuery("Select * From statement");
assertEquals("Statement.getResultSet", rs, st.getResultSet());
assertRowCount(5,rs);
assertRowCount(4,"Select top 4 * From statement");
assertRowCount(3,"Select * From statement Limit 3");
assertRowCount(2,"Select * From statement Order By c ASC Limit 2");
assertRowCount(0,"Select top 0 * From statement");
st = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
rs = st.executeQuery("Select Top 0 * From statement");
assertFalse( "last()", rs.last() );
PreparedStatement pr = con.prepareStatement("Select * From statement");
pr.setMaxRows(6);
rs = pr.executeQuery();
assertEquals("PreparedStatement.getResultSet", rs, pr.getResultSet());
assertRowCount(6,rs);
pr.setMaxRows(3);
rs = pr.executeQuery();
assertRowCount(3,rs);
pr.setMaxRows(4);
rs = pr.executeQuery();
assertRowCount(4,rs);
}
public void testMoreResults() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs = st.executeQuery("Select * From statement");
assertEquals( "getResultSet()", rs, st.getResultSet() );
assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
assertFalse( st.getMoreResults() );
try{
rs.next();
fail("ResultSet should be closed");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
assertNull( "getResultSet()", st.getResultSet() );
assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
rs = st.executeQuery("Select * From statement");
assertEquals( "getResultSet()", rs, st.getResultSet() );
assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
assertFalse( st.getMoreResults(Statement.KEEP_CURRENT_RESULT) );
assertTrue(rs.next());
assertNull( "getResultSet()", st.getResultSet() );
assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
int count = st.executeUpdate("Update statement set c = c");
assertTrue( "Update Erfolgreich", count>0 );
assertNull( "getResultSet()", st.getResultSet() );
assertEquals( "getUpdateCount()", count, st.getUpdateCount() );
assertFalse( st.getMoreResults() );
assertNull( "getResultSet()", st.getResultSet() );
assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
}
public void testGetConnection() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
assertEquals(con, st.getConnection() );
}
public void testFetch() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.setFetchDirection(ResultSet.FETCH_FORWARD);
assertEquals( st.getFetchDirection(), ResultSet.FETCH_FORWARD);
st.setFetchDirection(ResultSet.FETCH_REVERSE);
assertEquals( st.getFetchDirection(), ResultSet.FETCH_REVERSE);
st.setFetchSize(123);
assertEquals( st.getFetchSize(), 123);
}
public void testGeneratedKeys() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
ResultSet rs;
st.execute("Insert Into statement(c) Values('key1')", Statement.NO_GENERATED_KEYS);
try{
st.getGeneratedKeys();
fail("NO_GENERATED_KEYS");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
assertEquals("UpdateCount", 1, st.getUpdateCount());
assertNull("getResultSet", st.getResultSet());
st.execute("Insert Into statement(c) Values('key2')", Statement.RETURN_GENERATED_KEYS);
rs = st.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertTrue(rs.next());
assertEqualsRsValue( new Long(rs.getLong(1)), rs, false );
assertFalse(rs.next());
assertEquals(1,st.executeUpdate("Insert Into statement(c) Values('key3')", Statement.RETURN_GENERATED_KEYS));
rs = st.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
st.execute("Insert Into statement(c) Values('key4')", new int[]{2,1});
rs = st.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
assertRowCount(1,rs);
assertEquals(1,st.executeUpdate("Insert Into statement(c) Values('key5')", new int[]{2}));
rs = st.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
st.execute("Insert Into statement(c) Values('key6')", new String[]{"c","i"});
rs = st.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
assertRowCount(1,rs);
assertEquals(1,st.executeUpdate("Insert Into statement(c) Values('key7')", new String[]{"i"}));
rs = st.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
}
public void testGeneratedKeysWithPrepare() throws Exception{
Connection con = AllTests.getConnection();
ResultSet rs;
PreparedStatement pr = con.prepareStatement("Insert Into statement(c) Values('key1')", Statement.NO_GENERATED_KEYS);
pr.execute();
try{
pr.getGeneratedKeys();
fail("NO_GENERATED_KEYS");
}catch(SQLException ex){
assertSQLException("01000", 0, ex);
}
assertEquals("UpdateCount", 1, pr.getUpdateCount());
assertNull("getResultSet", pr.getResultSet());
pr.close();
pr = con.prepareStatement("Insert Into statement(c) Values('key2')", Statement.RETURN_GENERATED_KEYS);
pr.execute();
rs = pr.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
pr = con.prepareStatement("Insert Into statement(c) Values('key3')", Statement.RETURN_GENERATED_KEYS);
assertEquals(1,pr.executeUpdate());
rs = pr.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
pr = con.prepareStatement("Insert Into statement(c) Values('key4')", new int[]{2,1});
pr.execute();
rs = pr.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
assertRowCount(1,rs);
pr = con.prepareStatement("Insert Into statement(c) Values('key5')", new int[]{2});
assertEquals(1,pr.executeUpdate());
rs = pr.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
pr = con.prepareStatement("Insert Into statement(c) Values('key6')", new String[]{"c","i"});
pr.execute();
rs = pr.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
assertRowCount(1,rs);
pr = con.prepareStatement("Insert Into statement(c) Values('key7')", new String[]{"i"});
assertEquals(1,pr.executeUpdate());
rs = pr.getGeneratedKeys();
assertNotNull("RETURN_GENERATED_KEYS", rs);
assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
assertRowCount(1,rs);
}
public void testResultSetType() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, st.getResultSetType());
assertEquals(ResultSet.CONCUR_UPDATABLE, st.getResultSetConcurrency());
ResultSet rs = st.executeQuery("Select * From statement");
assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
}
public void testOther() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.clearWarnings();
assertNull(st.getWarnings());
st.setQueryTimeout(5);
assertEquals("QueryTimeout", 5, st.getQueryTimeout() );
st.setMaxFieldSize(100);
assertEquals("MaxFieldSize", 100, st.getMaxFieldSize() );
}
public void testTruncate() throws Exception{
Connection con = AllTests.getConnection();
Statement st = con.createStatement();
st.execute("Truncate table statement");
assertRowCount(0, "Select * From statement");
}
}
package smallsql.database;
abstract class ExpressionFunctionReturnFloat extends ExpressionFunction {
boolean isNull() throws Exception{
return param1.isNull();
}
final boolean getBoolean() throws Exception{
return getDouble() != 0;
}
final int getInt() throws Exception{
return (int)getDouble();
}
final long getLong() throws Exception{
return (long)getDouble();
}
final float getFloat() throws Exception{
return (float)getDouble();
}
long getMoney() throws Exception{
return Utils.doubleToMoney(getDouble());
}
final MutableNumeric getNumeric() throws Exception{
if(isNull()) return null;
double value = getDouble();
if(Double.isInfinite(value) || Double.isNaN(value))
return null;
return new MutableNumeric(value);
}
final Object getObject() throws Exception{
if(isNull()) return null;
return new Double(getDouble());
}
final String getString() throws Exception{
Object obj = getObject();
if(obj == null) return null;
return obj.toString();
}
final int getDataType() {
return SQLTokenizer.FLOAT;
}
}
package smallsql.database;
class JoinScrollIndex extends JoinScroll{
private final int compare;
Expressions leftEx;
Expressions rightEx;
private Index index;
private LongTreeList rowList;
private final LongTreeListEnum longListEnum = new LongTreeListEnum();
JoinScrollIndex( int joinType, RowSource left, RowSource right, Expressions leftEx, Expressions rightEx, int compare)
throws Exception{
super( joinType, left, right, null);
this.leftEx = leftEx;
this.rightEx = rightEx;
this.compare = compare;
createIndex(rightEx);
}
private void createIndex(Expressions rightEx) throws Exception{
index = new Index(false);
right.beforeFirst();
while(right.next()){
index.addValues(right.getRowPosition(), rightEx);
}
}
boolean next() throws Exception{
switch(compare){
case ExpressionArithmetic.EQUALS:
return nextEquals();
default:
throw new Error("Compare operation not supported:" + compare);
}
}
private boolean nextEquals() throws Exception{
if(rowList != null){
long rowPosition = rowList.getNext(longListEnum);
if(rowPosition != -1){
right.setRowPosition(rowPosition);
return true;
}
rowList = null;
}
Object rows;
do{
if(!left.next()){
return false;
}
rows = index.findRows(leftEx, false, null);
}while(rows == null);
if(rows instanceof Long){
right.setRowPosition(((Long)rows).longValue());
}else{
rowList = (LongTreeList)rows;
longListEnum.reset();
right.setRowPosition(rowList.getNext(longListEnum));
}
return true;
}
}
package smallsql.junit;
import java.sql.*;
public class TestDeleteUpdate extends BasicTestCase {
public TestDeleteUpdate() {
super();
}
public TestDeleteUpdate(String name) {
super(name);
}
public void testDelete() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"testDelete");
Statement st = con.createStatement();
st.execute("create table testDelete(a int default 15)");
for(int i=0; i<10; i++){
st.execute("Insert into testDelete Values("+i+")");
}
assertRowCount( 10, "Select * from testDelete");
st.execute("delete from testDelete Where a=3");
assertRowCount( 9, "Select * from testDelete");
st.execute("delete from testDelete Where a<5");
assertRowCount( 5, "Select * from testDelete");
st.execute("delete from testDelete");
assertRowCount( 0, "Select * from testDelete");
dropTable(con,"testDelete");
}
public void testUpdate1() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"testUpdate");
Statement st = con.createStatement();
st.execute("create table testUpdate(id int default 15, value int)");
for(int i=0; i<10; i++){
st.execute("Insert into testUpdate Values("+i+','+i+")");
}
assertRowCount( 10, "Select * from testUpdate");
int updateCount;
updateCount = st.executeUpdate("update testUpdate set value=103 Where id=3");
assertEqualsRsValue( new Integer(103), "Select value from testUpdate Where id=3");
assertRowCount( 10, "Select value from testUpdate");
assertEquals( 1, updateCount);
updateCount = st.executeUpdate("update testUpdate set value=104 Where id=3");
assertEqualsRsValue( new Integer(104), "Select value from testUpdate Where id=3");
assertRowCount( 10, "Select value from testUpdate");
assertEquals( 1, updateCount);
updateCount = st.executeUpdate("delete from testUpdate Where id=3");
assertRowCount( 9, "Select * from testUpdate");
assertEquals( 1, updateCount);
updateCount = st.executeUpdate("update testUpdate set value=27 Where id<5");
assertEquals( 4, updateCount);
dropTable(con,"testUpdate");
}
public void testUpdate2() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"testUpdate");
Statement st = con.createStatement();
st.execute("create table testUpdate(id int default 15, value1 varchar(100), value2 int)");
for(int i=0; i<10; i++){
st.execute("Insert into testUpdate Values("+i+','+(i*100)+','+i+")");
}
assertRowCount( 10, "Select * from testUpdate");
st.execute("update testUpdate set value1=13 Where id=3");
assertEqualsRsValue( "13", "Select value1 from testUpdate Where id=3");
assertRowCount( 10, "Select * from testUpdate");
st.execute("update testUpdate set value1=1040 Where id=3");
assertEqualsRsValue( "1040", "Select value1 from testUpdate Where id=3");
assertRowCount( 10, "Select * from testUpdate");
st.execute("update testUpdate set value1=10400 Where id=3");
assertEqualsRsValue( "10400", "Select value1 from testUpdate Where id=3");
assertRowCount( 10, "Select * from testUpdate");
st.execute("update testUpdate set value1=13,id=3 Where id=3");
assertEqualsRsValue( "13", "Select value1 from testUpdate Where id=3");
assertRowCount( 10, "Select * from testUpdate");
st.execute("delete from testUpdate Where id=3");
assertRowCount( 9, "Select * from testUpdate");
dropTable(con,"testUpdate");
}
public void testUpdateMultiTables() throws Exception{
Connection con = AllTests.getConnection();
dropTable(con,"testUpdate1");
dropTable(con,"testUpdate2");
Statement st = con.createStatement();
st.execute("create table testUpdate1(id1 int, value1 varchar(100))");
st.execute("create table testUpdate2(id2 int, value2 varchar(100))");
st.execute("Insert into testUpdate1 Values(11, 'qwert1')");
st.execute("Insert into testUpdate2 Values(11, 'qwert2')");
st.execute("update testUpdate1 inner join testUpdate2 on id1=id2 Set value1=value1+'update', value2=value2+'update'");
ResultSet rs = st.executeQuery("Select * From testUpdate1 inner join testUpdate2 on id1=id2");
assertTrue( rs.next() );
assertEquals( "qwert1update", rs.getString("value1"));
assertEquals( "qwert2update", rs.getString("value2"));
dropTable(con,"testUpdate1");
dropTable(con,"testUpdate2");
}
}
package smallsql.database;
import smallsql.database.language.Language;
class Scrollable extends RowSource {
private int rowIdx;
Scrollable(RowSource rowSource){
this.rowSource = rowSource;
}
final boolean isScrollable(){
return true;
}
void beforeFirst() throws Exception {
rowIdx = -1;
rowSource.beforeFirst();
}
boolean isBeforeFirst(){
return rowIdx == -1 || rowList.size() == 0;
}
boolean isFirst(){
return rowIdx == 0 && rowList.size()>0;
}
boolean first() throws Exception {
rowIdx = -1;
return next();
}
boolean previous() throws Exception{
if(rowIdx > -1){
rowIdx--;
if(rowIdx > -1 && rowIdx < rowList.size()){
rowSource.setRowPosition( rowList.get(rowIdx) );
return true;
}
}
rowSource.beforeFirst();
return false;
}
boolean next() throws Exception {
if(++rowIdx < rowList.size()){
rowSource.setRowPosition( rowList.get(rowIdx) );
return true;
}
final boolean result = rowSource.next();
if(result){
rowList.add( rowSource.getRowPosition());
return true;
}
rowIdx = rowList.size(); 
return false;
}
boolean last() throws Exception{
afterLast();
return previous();
}
boolean isLast() throws Exception{
if(rowIdx+1 != rowList.size()){
return false;
}
boolean isNext = next();
previous();
return !isNext && (rowIdx+1 == rowList.size() && rowList.size()>0);
}
boolean isAfterLast() throws Exception{
if(rowIdx >= rowList.size()) return true;
if(isBeforeFirst() && rowList.size() == 0){
next();
previous();
if(rowList.size() == 0) return true;
}
return false;
}
void afterLast() throws Exception {
if(rowIdx+1 < rowList.size()){
rowIdx = rowList.size()-1;
rowSource.setRowPosition( rowList.get(rowIdx) );
}
while(next()){
}
boolean absolute(int row) throws Exception{
if(row == 0)
throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
if(row < 0){
afterLast();
rowIdx = rowList.size() + row;
if(rowIdx < 0){
beforeFirst();
return false;
}else{
rowSource.setRowPosition( rowList.get(rowIdx) );
return true;
}
}
if(row <= rowList.size()){
rowIdx = row-1;
rowSource.setRowPosition( rowList.get(rowIdx) );
return true;
}
rowIdx = rowList.size()-1;
if(rowIdx >= 0)
rowSource.setRowPosition( rowList.get(rowIdx) );
boolean result;
while((result = next()) && row-1 > rowIdx){
return result;
}
boolean relative(int rows) throws Exception{
int newRow = rows + rowIdx + 1;
if(newRow <= 0){
beforeFirst();
return false;
}else{
return absolute(newRow);
}
}
int getRow() throws Exception {
if(rowIdx >= rowList.size()) return 0;
return rowIdx + 1;
}
long getRowPosition() {
return rowIdx;
}
void setRowPosition(long rowPosition) throws Exception {
rowIdx = (int)rowPosition;
}
final boolean rowInserted(){
return rowSource.rowInserted();
}
final boolean rowDeleted(){
return rowSource.rowDeleted();
}
void nullRow() {
rowSource.nullRow();
rowIdx = -1;
}
void noRow() {
rowSource.noRow();
rowIdx = -1;
}
void execute() throws Exception{
rowSource.execute();
rowList.clear();
rowIdx = -1;
}
boolean isExpressionsFromThisRowSource(Expressions columns){
return rowSource.isExpressionsFromThisRowSource(columns);
}
}
package smallsql.database;
final class ExpressionFunctionASin extends ExpressionFunctionReturnFloat {
final int getFunction(){ return SQLTokenizer.ASIN; }
final double getDouble() throws Exception{
if(isNull()) return 0;
return Math.asin( param1.getDouble() );
}
}
package smallsql.database;
final class Expressions {
private int size;
private Expression[] data;
Expressions(){
data = new Expression[16];
}
Expressions(int initSize){
data = new Expression[initSize];
}
final int size(){
return size;
}
final void setSize(int newSize){
for(int i=newSize; i<size; i++) data[i] = null;
size = newSize;
if(size>data.length) resize(newSize);
}
final Expression get(int idx){
if (idx >= size)
throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
return data[idx];
}
final void add(Expression expr){
if(size >= data.length ){
resize(size << 1);
}
data[size++] = expr;
}
final void add(int idx, Expression expr){
if(size >= data.length ){
resize(size << 1);
}
System.arraycopy( data, idx, data, idx+1, (size++)-idx);
data[idx] = expr;
}
final void addAll(Expressions cols){
int count = cols.size();
if(size + count >= data.length ){
resize(size + count);
}
System.arraycopy( cols.data, 0, data, size, count);
size += count;
}
final void clear(){
size = 0;
}
final void remove(int idx){
System.arraycopy( data, idx+1, data, idx, (--size)-idx);
}
final void set(int idx, Expression expr){
data[idx] = expr;
}
final int indexOf(Expression expr) {
if (expr == null) {
for (int i = 0; i < size; i++)
if (data[i]==null)
return i;
} else {
for (int i = 0; i < size; i++)
if (expr.equals(data[i]))
return i;
}
return -1;
}
final void toArray(Expression[] array){
System.arraycopy( data, 0, array, 0, size);
}
final Expression[] toArray(){
Expression[] array = new Expression[size];
System.arraycopy( data, 0, array, 0, size);
return array;
}
private final void resize(int newSize){
Expression[] dataNew = new Expression[newSize];
System.arraycopy(data, 0, dataNew, 0, size);
data = dataNew;
}
}
package smallsql.database;
import java.math.*;
class MutableNumeric extends Number implements Mutable{
private static final long serialVersionUID = -750525164208565056L;
private int[] value;
private int scale;
private int signum;
MutableNumeric(byte[] complement){
setValue(complement);
}
private void setValue(byte[] complement){
int length = complement.length;
if(length == 0){
value   = EMPTY_INTS;
signum  = 0;
return;
}
value = new int[ (length + 3) / 4 ];
if(complement[0] < 0){
negate( complement );
signum = -1;
}else{
signum = 0;
for(int i=0; i<complement.length; i++)
if(complement[i] != 0){
signum = 1;
break;
}
}
for(int v=value.length-1; v>=0; v--){
int temp = 0;
for(int i=0; i<4 && 0<length; i++){
temp |= (complement[ --length ] & 0xFF) << (i*8);
}
value[v] = temp;
}
}
MutableNumeric(int complement){
if(complement == 0){
signum = 0;
value = EMPTY_INTS;
}else{
value = new int[1];
if(complement < 0){
value[0] = -complement;
signum = -1;
}else{
value[0] = complement;
signum = 1;
}
}
}
MutableNumeric(int complement, int scale){
this( complement );
this.scale = scale;
}
MutableNumeric(long complement){
if(complement == 0){
signum = 0;
value = EMPTY_INTS;
}else{
value = new int[2];
if(complement < 0){
value[0] = (int)(~(complement >> 32));
value[1] = (int)(-complement);
signum = -1;
}else{
value[0] = (int)(complement >> 32);
value[1] = (int)complement;
signum = 1;
}
}
}
MutableNumeric(long complement, int scale){
this( complement );
this.scale = scale;
}
MutableNumeric(double val){
this( new BigDecimal( String.valueOf(val) ) );
}
MutableNumeric(float val){
this( new BigDecimal( String.valueOf(val) ) );
}
MutableNumeric(String val){
this( new BigDecimal( val ) );
}
MutableNumeric( BigDecimal big ){
this(big.unscaledValue().toByteArray() );
scale   = big.scale();
}
MutableNumeric(int signum, int[] value, int scale){
this.signum = signum;
this.value  = value;
this.scale  = scale;
}
MutableNumeric(MutableNumeric numeric){
this.signum = numeric.signum;
this.value  = new int[numeric.value.length];
System.arraycopy(numeric.value, 0, value, 0, value.length);
this.scale  = numeric.scale;
}
int[] getInternalValue(){
return value;
}
void add(MutableNumeric num){
if(num.scale < scale){
num.setScale(scale);
}else
if(num.scale > scale){
setScale(num.scale);
}
add( num.signum, num.value );
}
private void add( int sig2, int[] val2){
if(val2.length > value.length){
int[] temp = val2;
val2 = value;
value = temp;
int tempi = signum;
signum = sig2;
sig2 = tempi;
}
if(signum != sig2)
sub(val2);
else
add(val2);
}
private void add( int[] val2){
long temp = 0;
int v1 = value.length;
for(int v2 = val2.length; v2>0; ){
temp = (value[--v1] & 0xFFFFFFFFL) + (val2 [--v2] & 0xFFFFFFFFL) + (temp >>> 32);
value[v1] = (int)temp;
}
boolean uebertrag = (temp >>> 32) != 0;
while(v1 > 0 && uebertrag)
uebertrag = (value[--v1] = value[v1] + 1) == 0;
if(uebertrag){
resizeValue(1);
}
}
private void resizeValue(int highBits){
int val[] = new int[value.length+1];
val[0] = highBits;
System.arraycopy(value, 0, val, 1, value.length);
value = val;
}
void sub(MutableNumeric num){
if(num.scale < scale){
num.setScale(scale);
}else
if(num.scale > scale){
setScale(num.scale);
}
add( -num.signum, num.value );
}
private void sub(int[] val2){
long temp = 0;
int v1 = value.length;
for(int v2 = val2.length; v2>0; ){
temp = (value[--v1] & 0xFFFFFFFFL) - (val2 [--v2] & 0xFFFFFFFFL) + (temp >>>= 32);
value[v1] = (int)temp;
}
boolean uebertrag = (temp >>> 32) != 0;
while(v1 > 0 && uebertrag)
uebertrag = (value[--v1] = value[v1] - 1) == -1;
if(uebertrag){
signum = -signum;
int last = value.length-1;
for(int i=0; i<=last; i++){
value[i] = (i == last) ? -value[i] : ~value[i];
}
}
}
void mul(MutableNumeric num){
BigDecimal big = toBigDecimal().multiply(num.toBigDecimal() );
setValue( big.unscaledValue().toByteArray() );
scale = big.scale();
signum = big.signum();
}
final void mul(int factor){
if(factor < 0){
factor = - factor;
signum = -signum;
}
long carryover = 0;
for(int i = value.length-1; i>=0; i--){
long v = (value[i] & 0xFFFFFFFFL) * factor + carryover;
value[i] = (int)v;
carryover = v >> 32;
}
if(carryover > 0){
resizeValue( (int)carryover );
}
}
void div(MutableNumeric num){
int newScale = Math.max(scale+5, num.scale +4);
BigDecimal big = toBigDecimal().divide(num.toBigDecimal(), newScale, BigDecimal.ROUND_HALF_EVEN);
setValue( big.unscaledValue().toByteArray() );
scale = big.scale();
signum = big.signum();
}
final void div(int quotient){
mul(100000);
scale += 5;
divImpl(quotient);
}
final private void divImpl(int quotient){
if(quotient == 1) return;
if(quotient < 0){
quotient = - quotient;
signum = -signum;
}
int valueLength = value.length;
long carryover = 0;
for(int i = 0; i<valueLength; i++){
long v = (value[i] & 0xFFFFFFFFL) + carryover;
value[i] = (int)(v / quotient);
carryover = ((v % quotient) << 32);
}
carryover /= quotient;
if(carryover > 2147483648L || 
(carryover == 2147483648L && (value[valueLength-1] % 2 == 1))){
int i = valueLength-1;
boolean isCarryOver = true;
while(i >= 0 && isCarryOver)
isCarryOver = (value[i--] += 1) == 0;
}
if(valueLength>1 && value[0] == 0){
int[] temp = new int[valueLength-1];
System.arraycopy(value, 1, temp, 0, valueLength-1);
value = temp;
}
}
void mod(MutableNumeric num){
num = new MutableNumeric( doubleValue() % num.doubleValue() );
value = num.value;
scale = num.scale;
signum = num.signum;
}
int getScale(){
return scale;
}
void setScale(int newScale){
if(newScale == scale) return;
int factor = 1;
if(newScale > scale){
for(;newScale>scale; scale++){
factor *=10;
if(factor == 1000000000){
mul(factor);
factor = 1;
}
}
mul(factor);
}else{
for(;newScale<scale; scale--){
factor *=10;
if(factor == 1000000000){
divImpl(factor);
factor = 1;
}
}
divImpl(factor);
}
}
int getSignum() {
return signum;
}
void setSignum(int signum){
this.signum = signum;
}
void floor(){
int oldScale = scale;
setScale(0);
setScale(oldScale);
}
private void negate(byte[] complement){
int last = complement.length-1;
for(int i=0; i<=last; i++){
complement[i] = (byte)( (i == last) ? -complement[i] : ~complement[i]);
}
while(complement[last] == 0){
last--;
complement[last]++;
}
}
byte[] toByteArray(){
if(signum == 0) return EMPTY_BYTES;
byte[] complement;
int offset;
int v = 0;
while(v < value.length && value[v] == 0) v++;
if (v == value.length) return EMPTY_BYTES;
if(value[v] < 0){
complement = new byte[(value.length-v)*4 + 4];
if(signum < 0)
complement[0] = complement[1] = complement[2] = complement[3] = -1;
offset = 4;
}else{
complement = new byte[(value.length-v)*4];
offset = 0;
}
int last = value.length-1;
for(; v <= last; v++){
int val = (signum>0) ? value[v] : (v == last) ? -value[v] : ~value[v];
complement[offset++] = (byte)(val >> 24);
complement[offset++] = (byte)(val >> 16);
complement[offset++] = (byte)(val >> 8);
complement[offset++] = (byte)(val);
}
return complement;
}
public int intValue(){
return Utils.long2int(longValue());
}
public long longValue(){
if(value.length == 0 || signum == 0){
return 0;
}else{
if (value.length == 1 && (value[0] > 0)){
return Utils.double2long(value[0] / scaleDoubleFactor[scale] * signum);
}else
if (value.length == 1){
long temp = value[0] & 0xFFFFFFFFL;
return Utils.double2long(temp / scaleDoubleFactor[scale] * signum);
}else
if (value.length == 2 && (value[0] > 0)){
long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
return Utils.double2long(temp / scaleDoubleFactor[scale] * signum);
}else{
if(scale != 0){
MutableNumeric numeric = new MutableNumeric(this);
numeric.setScale(0);
return numeric.longValue();
}
return (signum > 0) ? Long.MAX_VALUE : Long.MIN_VALUE;
}
}
}
public float floatValue(){
if(value.length == 0 || signum == 0){
return 0;
}else{
if (value.length == 1 && (value[0] > 0)){
return value[0] / scaleFloatFactor[scale] * signum;
}else
if (value.length == 1){
long temp = value[0] & 0xFFFFFFFFL;
return temp / scaleFloatFactor[scale] * signum;
}else
if (value.length == 2 && (value[0] > 0)){
long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
return temp / scaleFloatFactor[scale] * signum;
}else{
return new BigDecimal( new BigInteger( toByteArray() ), scale ).floatValue();
}
}
}
public double doubleValue(){
if(value.length == 0 || signum == 0){
return 0;
}else{
if (value.length == 1 && (value[0] > 0)){
return value[0] / scaleDoubleFactor[scale] * signum;
}else
if (value.length == 1){
long temp = value[0] & 0xFFFFFFFFL;
return temp / scaleDoubleFactor[scale] * signum;
}else
if (value.length == 2 && (value[0] > 0)){
long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
return temp / scaleDoubleFactor[scale] * signum;
}else{
return new BigDecimal( new BigInteger( toByteArray() ), scale ).doubleValue();
}
}
}
public String toString(){
StringBuffer buf = new StringBuffer();
if(value.length == 0 || signum == 0){
buf.append( '0' );
}else{
if (value.length == 1 && (value[0] > 0)){
buf.append( Integer.toString(value[0]) );
}else
if (value.length == 1){
long temp = value[0] & 0xFFFFFFFFL;
buf.append( Long.toString( temp ) );
}else
if (value.length == 2 && (value[0] > 0)){
long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
buf.append( Long.toString( temp ) );
}else{
return new BigDecimal( new BigInteger( toByteArray() ), scale ).toString();
}
}
if(scale > 0){
while(buf.length() <= scale) buf.insert( 0, '0' );
buf.insert( buf.length() - scale, '.' );
}
if (signum < 0) buf.insert( 0, '-');
return buf.toString();
}
public int compareTo(MutableNumeric numeric){
return toBigDecimal().compareTo(numeric.toBigDecimal());
}
public boolean equals(Object obj){
if(!(obj instanceof MutableNumeric)) return false;
return compareTo((MutableNumeric)obj) == 0;
}
public BigDecimal toBigDecimal(){
if(signum == 0) return new BigDecimal( BigInteger.ZERO, scale);
return new BigDecimal( new BigInteger( toByteArray() ), scale );
}
public BigDecimal toBigDecimal(int newScale){
if(newScale == this.scale) return toBigDecimal();
return toBigDecimal().setScale( newScale, BigDecimal.ROUND_HALF_EVEN);
}
public Object getImmutableObject(){
return toBigDecimal();
}
private static final byte[] EMPTY_BYTES = new byte[0];
private static final int [] EMPTY_INTS  = new int [0];
private static final double[] scaleDoubleFactor = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000 };
private static final float[]  scaleFloatFactor =  { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000 };
}
package smallsql.database;
import java.io.*;
import smallsql.database.language.Language;
public class CommandDrop extends Command {
CommandDrop( Logger log, String catalog, String name, int type ){
super(log);
this.type 		= type;
this.catalog 	= catalog;
this.name 		= name;
}
void executeImpl(SSConnection con, SSStatement st) throws Exception {
switch(type){
case SQLTokenizer.DATABASE:
if(name.startsWith("file:"))
name = name.substring(5);
File dir = new File( name );
if(!dir.isDirectory() ||
!new File( dir, Utils.MASTER_FILENAME ).exists())
throw SmallSQLException.create(Language.DB_NONEXISTENT, name);
File files[] = dir.listFiles();
if(files != null)
for(int i=0; i<files.length; i++){
files[i].delete();
}
dir.delete();
break;
case SQLTokenizer.TABLE:
Database.dropTable( con, catalog, name );
break;
case SQLTokenizer.VIEW:
Database.dropView( con, catalog, name );
break;
case SQLTokenizer.INDEX:
case SQLTokenizer.PROCEDURE:
throw new java.lang.UnsupportedOperationException();
default:
throw new Error();
}
}
}
package smallsql.database;
public class ExpressionFunctionChar extends ExpressionFunctionReturnString {
final int getFunction() {
return SQLTokenizer.CHAR;
}
final String getString() throws Exception {
if(isNull()) return null;
char chr = (char)param1.getInt();
return String.valueOf(chr);
}
final int getDataType() {
return SQLTokenizer.CHAR;
}
final int getPrecision(){
return 1;
}
}
package smallsql.database;
abstract class DataSource extends RowSource{
abstract boolean isNull( int colIdx ) throws Exception;
abstract boolean getBoolean( int colIdx ) throws Exception;
abstract int getInt( int colIdx ) throws Exception;
abstract long getLong( int colIdx ) throws Exception;
abstract float getFloat( int colIdx ) throws Exception;
abstract double getDouble( int colIdx ) throws Exception;
abstract long getMoney( int colIdx ) throws Exception;
abstract MutableNumeric getNumeric( int colIdx ) throws Exception;
abstract Object getObject( int colIdx ) throws Exception;
abstract String getString( int colIdx ) throws Exception;
abstract byte[] getBytes( int colIdx ) throws Exception;
abstract int getDataType( int colIdx );
boolean init( SSConnection con ) throws Exception{return false;}
String getAlias(){return null;}
abstract TableView getTableView();
boolean isExpressionsFromThisRowSource(Expressions columns){
for(int i=0; i<columns.size(); i++){
ExpressionName expr = (ExpressionName)columns.get(i);
if(this != expr.getDataSource()){
return false;
}
}
return true;
}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import smallsql.database.language.Language;
class View extends TableView{
final String sql;
final CommandSelect commandSelect;
View(SSConnection con, String name, FileChannel raFile, long offset) throws Exception{
super( name, new Columns() );
StorePage storePage = new StorePage( null, -1, raFile, offset);
StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.SELECT, offset);
sql = store.readString();
int type;
while((type = store.readInt()) != 0){
int offsetInPage = store.getCurrentOffsetInPage();
int size = store.readInt();
switch(type){
}
store.setCurrentOffsetInPage(offsetInPage + size);
}
raFile.close();
commandSelect = (CommandSelect)new SQLParser().parse(con, sql);
createColumns(con);
}
View(Database database, SSConnection con, String name, String sql) throws Exception{
super( name, new Columns() );
this.sql  = sql;
this.commandSelect = null;
write(database, con);
}
View(SSConnection con, CommandSelect commandSelect) throws Exception{
super("UNION", new Columns());
this.sql = null;
this.commandSelect = commandSelect;
createColumns(con);
}
private void createColumns(SSConnection con) throws Exception{
commandSelect.compile(con);
Expressions exprs = commandSelect.columnExpressions;
for(int c=0; c<exprs.size(); c++){
Expression expr = exprs.get(c);
if(expr instanceof ExpressionName){
Column column = ((ExpressionName)expr).getColumn().copy();
column.setName( expr.getAlias() );
columns.add( column );
}else{
columns.add( new ColumnExpression(expr));
}
}
}
static void drop(Database database, String name) throws Exception{
File file = new File( Utils.createTableViewFileName( database, name ) );
boolean ok = file.delete();
if(!ok) throw SmallSQLException.create(Language.VIEW_CANTDROP, name);
}
private void write(Database database, SSConnection con) throws Exception{
FileChannel raFile = createFile( con, database );
StorePage storePage = new StorePage( null, -1, raFile, 8);
StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.CREATE, 8);
store.writeString(sql);
store.writeInt( 0 ); 
store.writeFinsh(null);
raFile.close();
}
@Override
void writeMagic(FileChannel raFile) throws Exception{
ByteBuffer buffer = ByteBuffer.allocate(8);
buffer.putInt(MAGIC_VIEW);
buffer.putInt(TABLE_VIEW_VERSION);
buffer.position(0);
raFile.write(buffer);
}
}
package smallsql.database;
import java.math.BigDecimal;
import java.sql.*;
import smallsql.database.language.Language;
public class ExpressionValue extends Expression {
private Object value;
private int dataType;
private int length;
ExpressionValue(){
super(VALUE);
clear();
}
ExpressionValue(int type){
super(type);
switch(type){
case GROUP_BY:
case SUM:
case FIRST:
case LAST:
clear();
break;
case MIN:
case MAX:
break;
case COUNT:
value = new MutableInteger(0);
dataType = SQLTokenizer.INT;
break;
default: throw new Error();
}
}
ExpressionValue(Object value, int dataType ){
super(VALUE);
this.value      = value;
this.dataType   = dataType;
}
public boolean equals(Object expr){
if(!super.equals(expr)) return false;
if(!(expr instanceof ExpressionValue)) return false;
Object v = ((ExpressionValue)expr).value;
if(v == value) return true;
if(value == null) return false;
return value.equals(v);
}
void accumulate(Expression expr) throws Exception{
int type = getType();
if(type != GROUP_BY) expr = expr.getParams()[0];
switch(type){
case GROUP_BY:
case FIRST:
if(isEmpty()) set( expr.getObject(), expr.getDataType() );
break;
case LAST:
set( expr.getObject(), expr.getDataType() );
break;
case COUNT:
if(!expr.isNull()) ((MutableInteger)value).value++;
break;
case SUM:
if(isEmpty()){
initValue( expr );
}else
switch(dataType){
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
((MutableInteger)value).value += expr.getInt();
break;
case SQLTokenizer.BIGINT:
((MutableLong)value).value += expr.getLong();
break;
case SQLTokenizer.REAL:
((MutableFloat)value).value += expr.getFloat();
break;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
((MutableDouble)value).value += expr.getDouble();
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
MutableNumeric newValue = expr.getNumeric();
if(newValue != null)
((MutableNumeric)value).add( newValue );
break;
case SQLTokenizer.MONEY:
((Money)value).value += expr.getMoney();
break;
default: throw SmallSQLException.create(Language.UNSUPPORTED_TYPE_SUM, SQLTokenizer.getKeyWord(dataType));
}
break;
case MAX:
if(value == null){
if(expr.isNull())
dataType = expr.getDataType();
else
initValue( expr );
}else if(!expr.isNull()){
switch(dataType){
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
((MutableInteger)value).value = Math.max( ((MutableInteger)value).value, expr.getInt());
break;
case SQLTokenizer.BIGINT:
((MutableLong)value).value = Math.max( ((MutableLong)value).value, expr.getLong());
break;
case SQLTokenizer.REAL:
((MutableFloat)value).value = Math.max( ((MutableFloat)value).value, expr.getFloat());
break;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
((MutableDouble)value).value = Math.max( ((MutableDouble)value).value, expr.getDouble());
break;
case SQLTokenizer.CHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.LONGVARCHAR:
String str = expr.getString();
if(String.CASE_INSENSITIVE_ORDER.compare( (String)value, str ) < 0) 
value = str;
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
MutableNumeric newValue = expr.getNumeric();
if(((MutableNumeric)value).compareTo( newValue ) < 0)
value = newValue;
break;
case SQLTokenizer.MONEY:
((Money)value).value = Math.max( ((Money)value).value, expr.getMoney());
break;
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
((DateTime)value).time = Math.max( ((DateTime)value).time, expr.getLong());
break;
case SQLTokenizer.UNIQUEIDENTIFIER:
String uuidStr = expr.getString();
if (uuidStr.compareTo( (String)value) > 0) value = uuidStr;
break;
default:
String keyword = SQLTokenizer.getKeyWord(dataType);
throw SmallSQLException.create(Language.UNSUPPORTED_TYPE_MAX, keyword);
}
}
break;
case MIN:
if(value == null){
if(expr.isNull())
dataType = expr.getDataType();
else
initValue( expr );
}else if(!expr.isNull()){
switch(dataType){
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
((MutableInteger)value).value = Math.min( ((MutableInteger)value).value, expr.getInt());
break;
case SQLTokenizer.BIGINT:
((MutableLong)value).value = Math.min( ((MutableLong)value).value, expr.getLong());
break;
case SQLTokenizer.REAL:
((MutableFloat)value).value = Math.min( ((MutableFloat)value).value, expr.getFloat());
break;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
((MutableDouble)value).value = Math.min( ((MutableDouble)value).value, expr.getDouble());
break;
case SQLTokenizer.CHAR:
case SQLTokenizer.VARCHAR:
case SQLTokenizer.LONGVARCHAR:
String str = expr.getString();
if(String.CASE_INSENSITIVE_ORDER.compare( (String)value, str ) > 0) 
value = str;
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
MutableNumeric newValue = expr.getNumeric();
if(((MutableNumeric)value).compareTo( newValue ) > 0)
value = newValue;
break;
case SQLTokenizer.MONEY:
((Money)value).value = Math.min( ((Money)value).value, expr.getMoney());
break;
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
((DateTime)value).time = Math.min( ((DateTime)value).time, expr.getLong());
break;
default: throw new Error(""+dataType);
}
}
break;
default: throw new Error();
}
}
private void initValue(Expression expr) throws Exception{
dataType = expr.getDataType();
switch(dataType){
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
value = new MutableInteger(expr.getInt());
break;
case SQLTokenizer.BIGINT:
value = new MutableLong(expr.getLong());
break;
case SQLTokenizer.REAL:
value = new MutableFloat(expr.getFloat());
break;
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
value = new MutableDouble(expr.getDouble());
break;
case SQLTokenizer.SMALLMONEY:
case SQLTokenizer.MONEY:
value = Money.createFromUnscaledValue(expr.getMoney());
break;
case SQLTokenizer.NUMERIC:
case SQLTokenizer.DECIMAL:
value = new MutableNumeric(expr.getNumeric());
break;
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.SMALLDATETIME:
case SQLTokenizer.DATE:
case SQLTokenizer.TIME:
value = new DateTime(expr.getLong(), dataType);
break;
default:
value = expr.getObject();
}
}
private static final Object EMPTY = new Object();
final boolean isEmpty(){
return value == EMPTY;
}
final void clear(){
value = EMPTY;
}
final void set( Object value, int _dataType, int length ) throws SQLException{
set( value, _dataType );
this.length = length;
}
final void set( Object newValue, int newDataType ) throws SQLException{
this.value      = newValue;
this.dataType   = newDataType;
if(dataType < 0){
if(newValue == null)
this.dataType = SQLTokenizer.NULL;
else
if(newValue instanceof String)
this.dataType = SQLTokenizer.VARCHAR;
else
if(newValue instanceof Byte)
this.dataType = SQLTokenizer.TINYINT;
else
if(newValue instanceof Short)
this.dataType = SQLTokenizer.SMALLINT;
else
if(newValue instanceof Integer)
this.dataType = SQLTokenizer.INT;
else
if(newValue instanceof Long || newValue instanceof Identity)
this.dataType = SQLTokenizer.BIGINT;
else
if(newValue instanceof Float)
this.dataType = SQLTokenizer.REAL;
else
if(newValue instanceof Double)
this.dataType = SQLTokenizer.DOUBLE;
else
if(newValue instanceof Number)
this.dataType = SQLTokenizer.DECIMAL;
else
if(newValue instanceof java.util.Date){
DateTime dateTime;
this.value = dateTime = DateTime.valueOf((java.util.Date)newValue);
this.dataType = dateTime.getDataType();
}else
if(newValue instanceof byte[])
this.dataType = SQLTokenizer.VARBINARY;
else
if(newValue instanceof Boolean)
this.dataType = SQLTokenizer.BOOLEAN;
else
if(newValue instanceof Money)
this.dataType = SQLTokenizer.MONEY;
else
throw SmallSQLException.create(Language.PARAM_CLASS_UNKNOWN, newValue.getClass().getName());
}
}
final void set(ExpressionValue val){
this.value 		= val.value;
this.dataType	= val.dataType;
this.length		= val.length;
}
boolean isNull(){
return getObject() == null;
}
boolean getBoolean() throws Exception{
return getBoolean( getObject(), dataType );
}
static boolean getBoolean(Object obj, int dataType) throws Exception{
if(obj == null) return false;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return (obj.equals(Boolean.TRUE));
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
return ((Number)obj).intValue() != 0;
case SQLTokenizer.REAL:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
return ((Number)obj).doubleValue() != 0;
default: return Utils.string2boolean( obj.toString() );
}
}
int getInt() throws Exception{
return getInt( getObject(), dataType );
}
static int getInt(Object obj, int dataType) throws Exception{
if(obj == null) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return (obj == Boolean.TRUE) ? 1 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
case SQLTokenizer.REAL:
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:
return ((Number)obj).intValue();
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
return (int)((DateTime)obj).getTimeMillis();
default:
String str = obj.toString().trim();
try{
return Integer.parseInt( str );
}catch(Throwable th){
return (int)Double.parseDouble( str );
}
}
long getLong() throws Exception{
return getLong( getObject(), dataType);
}
static long getLong(Object obj, int dataType) throws Exception{
if(obj == null) return 0;
switch(dataType){
case SQLTokenizer.BIT:
case SQLTokenizer.BOOLEAN:
return (obj == Boolean.TRUE) ? 1 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
return ((Number)obj).longValue();
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
return ((DateTime)obj).getTimeMillis();
default:
String str = obj.toString();
if(str.indexOf('-') > 0 || str.indexOf(':') > 0)
return DateTime.parse(str);
try{
return Long.parseLong( str );
}catch(NumberFormatException e){
return (long)Double.parseDouble( str );
}
}
}
float getFloat() throws Exception{
return getFloat( getObject(), dataType);
}
static float getFloat(Object obj, int dataType) throws Exception{
if(obj == null) return 0;
switch(dataType){
case SQLTokenizer.BIT:
return (obj.equals(Boolean.TRUE)) ? 1 : 0;
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.FLOAT:
case SQLTokenizer.REAL:
case SQLTokenizer.MONEY:
return ((Number)obj).floatValue();
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
return ((DateTime)obj).getTimeMillis();
default: return Float.parseFloat( obj.toString() );
}
}
double getDouble() throws Exception{
return getDouble( getObject(), dataType);
}
static double getDouble(Object obj, int dataType) throws Exception{
if(obj == null) return 0;
switch(dataType){
case SQLTokenizer.BIT:
return (obj.equals(Boolean.TRUE)) ? 1 : 0;
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
case SQLTokenizer.DOUBLE:
case SQLTokenizer.MONEY:
return ((Number)obj).doubleValue();
case SQLTokenizer.TIMESTAMP:
case SQLTokenizer.TIME:
case SQLTokenizer.DATE:
case SQLTokenizer.SMALLDATETIME:
return ((DateTime)obj).getTimeMillis();
default: return Double.parseDouble( obj.toString() );
}
}
long getMoney() throws Exception{
return getMoney( getObject(), dataType );
}
static long getMoney(Object obj, int dataType) throws Exception{
if(obj == null) return 0;
switch(dataType){
case SQLTokenizer.BIT:
return (obj == Boolean.TRUE) ? 10000 : 0;
case SQLTokenizer.TINYINT:
case SQLTokenizer.SMALLINT:
case SQLTokenizer.INT:
case SQLTokenizer.BIGINT:
return ((Number)obj).longValue() * 10000;
case SQLTokenizer.REAL:
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
return Utils.doubleToMoney(((Number)obj).doubleValue());
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return ((Money)obj).value;
default: return Money.parseMoney( obj.toString() );
}
}
MutableNumeric getNumeric(){
return getNumeric( getObject(), dataType );
}
static MutableNumeric getNumeric(Object obj, int dataType){
if(obj == null) return null;
switch(dataType){
case SQLTokenizer.BIT:
return new MutableNumeric( (obj == Boolean.TRUE) ? 1 : 0);
case SQLTokenizer.INT:
return new MutableNumeric( ((Number)obj).intValue() );
case SQLTokenizer.BIGINT:
return new MutableNumeric( ((Number)obj).longValue() );
case SQLTokenizer.REAL:
float fValue = ((Number)obj).floatValue();
if(Float.isInfinite(fValue) || Float.isNaN(fValue))
return null;
return new MutableNumeric( fValue );
case SQLTokenizer.FLOAT:
case SQLTokenizer.DOUBLE:
double dValue = ((Number)obj).doubleValue();
if(Double.isInfinite(dValue) || Double.isNaN(dValue))
return null;
return new MutableNumeric( dValue );
case SQLTokenizer.MONEY:
case SQLTokenizer.SMALLMONEY:
return new MutableNumeric( ((Money)obj).value, 4 );
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:
if(obj instanceof MutableNumeric)
return (MutableNumeric)obj;
return new MutableNumeric( (BigDecimal)obj );
default: return new MutableNumeric( obj.toString() );
}
}
Object getObject(){
if(isEmpty()){
return null;
}
return value;
}
String getString(){
Object obj = getObject();
if(obj == null) return null;
if(dataType == SQLTokenizer.BIT){
return (obj == Boolean.TRUE) ? "1" : "0";
}
return obj.toString();
}
byte[] getBytes() throws Exception{
return getBytes( getObject(), dataType);
}
static byte[] getBytes(Object obj, int dataType) throws Exception{
if(obj == null) return null;
switch(dataType){
case SQLTokenizer.BINARY:
case SQLTokenizer.VARBINARY:
case SQLTokenizer.LONGVARBINARY:
return (byte[])obj;
case SQLTokenizer.VARCHAR:
case SQLTokenizer.CHAR:
case SQLTokenizer.NVARCHAR:
case SQLTokenizer.NCHAR:
return ((String)obj).getBytes();
case SQLTokenizer.UNIQUEIDENTIFIER:
return Utils.unique2bytes((String)obj);
case SQLTokenizer.INT:
return Utils.int2bytes( ((Number)obj).intValue() );
case SQLTokenizer.DOUBLE:
return Utils.double2bytes( ((Number)obj).doubleValue() );
case SQLTokenizer.REAL:
return Utils.float2bytes( ((Number)obj).floatValue() );
default: throw createUnsupportedConversion(dataType, obj, SQLTokenizer.VARBINARY);
}
}
final int getDataType(){
return dataType;
}
String getTableName(){
return null;
}
final int getPrecision(){
switch(dataType){
case SQLTokenizer.VARCHAR:
case SQLTokenizer.CHAR:
return ((String)value).length();
case SQLTokenizer.VARBINARY:
case SQLTokenizer.BINARY:
return ((byte[])value).length;
default:
return super.getPrecision();
}
}
int getScale(){
switch(dataType){
case SQLTokenizer.DECIMAL:
case SQLTokenizer.NUMERIC:
MutableNumeric obj = getNumeric();
return (obj == null) ? 0: obj.getScale();
default:
return getScale(dataType);
}
}
static SQLException createUnsupportedConversion( int fromDataType, Object obj, int toDataType ){
Object[] params = {
SQLTokenizer.getKeyWord(fromDataType),
obj,
SQLTokenizer.getKeyWord(toDataType)
};
return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION, params);
}
}
