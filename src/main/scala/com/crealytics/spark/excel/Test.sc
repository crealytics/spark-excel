import org.apache.poi.xssf.usermodel.XSSFTable


val table = new XSSFTable()
table.getCTTable.setRef("Foo[#All]")