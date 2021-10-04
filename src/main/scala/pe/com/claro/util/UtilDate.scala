package pe.com.claro.util

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

object UtilDate {


  def convertToDateCdv(fechaCadena: String): Date = {
    val format = "yyyyMMdd"
    val dateFormat = new SimpleDateFormat(format)
    val fDate = dateFormat.parse(fechaCadena.replace("-", ""))
    fDate
  }

  def convertToDateBdb(fechaCadena: String): Date = {
    val format = "yyyy-MM-dd"
    val dateFormat = new SimpleDateFormat(format)
    val fDate = dateFormat.parse(fechaCadena)
    fDate
  }

  /** Genera un objeto calendar a partir de una cadena de texto.
   *
   * @param fechaCadena : String que va a ser transformado a Calendar, debe tener el formato yyyy-MM-dd
   * @return un objeto java.util.Calendar
   */
  def convertToCalendar(fechaCadena: String): Calendar = {
    val format = "yyyyMMdd"
    val calendar = Calendar.getInstance
    val sdf = new SimpleDateFormat(format)
    calendar.setTime(sdf.parse(fechaCadena.replace("-", "")))
    calendar
  }

  def getYear(fechaCadena: String): String = {
    val dateFormat = "MM/dd/yyyy"
    val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
    val d = java.time.LocalDate.parse(fechaCadena, dtf)
    val year = d.getYear
    year.toString
  }

  /**
   * Usado para los templates de BDV
   * Método que obtiene la fecha inicial para los filtros por fecha. Considera los primeros 15 días del año como el año anterior y a partir de alli el primer día del año en curso
   *
   * @return String en formato yyyy-MM-dd
   */
  def getBeginDate(): String = {
    val hoy = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val primerDia = Calendar.getInstance
    primerDia.set(Calendar.DAY_OF_YEAR, 1)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val startDate = sdf.format(primerDia.getTime())
    val oldDate = LocalDate.parse(startDate, formatter)

    val currentDate = sdf.format(hoy.getTime())
    val newDate = LocalDate.parse(currentDate, formatter)

    val diferencia = newDate.toEpochDay() - oldDate.toEpochDay()

    hoy.set(Calendar.DAY_OF_YEAR, 1)
    hoy.set(Calendar.MONTH, 1);

    if (diferencia <= 30) {
      hoy.add(Calendar.YEAR, -1);
    }

    sdf.format(hoy.getTime())
  }

  /**
   * Usado para los templates de BDV
   * Método que obtiene la fecha final para los filtros por fecha. Considera los primeros 15 días del año como el ultimo día año anterior y a partir de alli el ultimo día del mes en curso
   *
   * @return String en formato yyyy-MM-dd
   */
  def getLastDate(): String = {
    val hoy = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val primerDia = Calendar.getInstance
    primerDia.set(Calendar.DAY_OF_YEAR, 1)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val startDate = sdf.format(primerDia.getTime())
    val oldDate = LocalDate.parse(startDate, formatter)

    val currentDate = sdf.format(hoy.getTime())
    val newDate = LocalDate.parse(currentDate, formatter)

    val diferencia = newDate.toEpochDay() - oldDate.toEpochDay()

    if (diferencia <= 30) {
      hoy.set(Calendar.DAY_OF_YEAR, 1)
      hoy.add(Calendar.DATE, -1);
    } else {
      // hoy.add(Calendar.MONTH, 1);
      hoy.set(Calendar.DAY_OF_MONTH, 1);
      hoy.add(Calendar.DATE, -1);
    }

    sdf.format(hoy.getTime())
  }

  /**
   * Usado para los templates de BDV
   * Método que obtiene el año del ejercicio. Considera los primeros 15 días del año como el año anterior y a partir de alli el primer día del año en curso
   *
   * @return String en formato yyyy-MM-dd
   */
  def getJobYear(): String = {
    val hoy = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val sdYear = new SimpleDateFormat("yyyy")

    val primerDia = Calendar.getInstance
    primerDia.set(Calendar.DAY_OF_YEAR, 1)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val startDate = sdf.format(primerDia.getTime())
    val oldDate = LocalDate.parse(startDate, formatter)

    val currentDate = sdf.format(hoy.getTime())
    val newDate = LocalDate.parse(currentDate, formatter)

    val diferencia = newDate.toEpochDay() - oldDate.toEpochDay()

    if (diferencia <= 30) {
      hoy.set(Calendar.DAY_OF_YEAR, 1)
      hoy.add(Calendar.DATE, -1);
    } else {
      // hoy.add(Calendar.MONTH, 1);
      hoy.set(Calendar.DAY_OF_MONTH, 1);
      hoy.add(Calendar.DATE, -1);
    }

    sdYear.format(hoy.getTime())
  }

  def getJobMonth(): String = {
    val hoy = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val sdMonth = new SimpleDateFormat("MM")

    val primerDia = Calendar.getInstance
    primerDia.set(Calendar.DAY_OF_YEAR, 1)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val startDate = sdf.format(primerDia.getTime())
    val oldDate = LocalDate.parse(startDate, formatter)

    val currentDate = sdf.format(hoy.getTime())
    val newDate = LocalDate.parse(currentDate, formatter)

    val diferencia = newDate.toEpochDay() - oldDate.toEpochDay()

    if (diferencia <= 30) {
      hoy.set(Calendar.DAY_OF_YEAR, 1)
      hoy.add(Calendar.MONTH, -1)
    } else {
      // hoy.add(Calendar.MONTH, 1);
      hoy.set(Calendar.DAY_OF_MONTH, 1)
      hoy.add(Calendar.DATE, 0)
    }

    sdMonth.format(hoy.getTime())
  }

}
