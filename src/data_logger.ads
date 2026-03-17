--  This package provides data logging Carlo Gavazzi Energy meter via
--  WaveShare RS455 to POE Ethernet converter. It assumes that energy meter
--  data is provided by an MQTT broker.

--  Author    : David Haley
--  Created   : 04/03/2026
--  Last Edit : 15/03/2026

--  20260315: Ported to MQTT_Client;

package Data_Logger is
   
   task Logger is
      -- Logger task declaration
      entry Start (Broker_Host, User_Name, Password, Topic : in String);
      entry Stop;
   end Logger;

end Data_Logger;
