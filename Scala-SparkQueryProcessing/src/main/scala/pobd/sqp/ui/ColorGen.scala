package pobd.sqp.ui

import java.awt.Color
import java.util.Random

/**
  * Created by daniel on 4/10/16.
  */
object ColorGen {
  var rand: Random = null

  def getRandomColor: Color ={
    if(rand == null) rand = new Random()

    val r = rand.nextFloat() / 2f + 0.5f
    val g = rand.nextFloat() / 2f + 0.5f
    val b = rand.nextFloat() / 2f + 0.5f

    new Color(r, g, b)
  }

  def getRandomRGBColor : String = {
    val color = getRandomColor

    val r = color.getRed
    val g = color.getGreen
    val b = color.getBlue

    "#%02X%02X%02X".format(r, g, b)
  }
}
