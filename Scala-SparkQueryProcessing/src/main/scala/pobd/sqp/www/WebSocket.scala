package pobd.sqp.www

import java.io._
import java.net.ServerSocket
import java.net.Socket
import java.util.ArrayList
import java.util.List


/**
  * Created by daniel on 4/6/16.
  */
class WebSocket(port : Int) {

  def p(msg : String): Unit ={
    println(msg)
  }

  def startWebSocket(): Unit ={
    var ws = new WebSocket
  }

  def startListen(): Unit ={
    var socket : ServerSocket = null

    try{
      socket = new ServerSocket(this.port)
      p(s">> Socket start at port ${this.port}")

      while(true){

        var client : Socket = socket.accept()
        var stream = client.getInputStream()
        val buffer : Array[Byte] = new Array[Byte](20)

        stream.read(buffer)
        val message : String = new String(buffer)

        println(message)

        val out = "Hello from the other side"
        p(s">> Sending $out")
        val output = new PrintStream(client.getOutputStream())
        output.println(out)
        output.flush()
        output.close()

        stream.close()
        client.close()
      }
    }
    catch {
      case p : Exception => p.printStackTrace()
    }

  }
}

object TestWebSocket{
  def main(args : Array[String]): Unit ={
    println("Hello world")
    var s = new WebSocket(9998)

    s.startListen()
  }
}