--  Program to be run as a service and create daily log files for the power
--  meter. The data is obtained from a MQTT broker with the appropriate topic.
--  Command Line arguments: Topic, Broker_Host User_Name Password;

--  Author    : David Haley
--  Created   : 15/02/2026
--  Last_Edit : 17/03/2026

--  20260315: Ported to MQTT_Client;
--  20260316: Ported to GNATCOLL.JSON

with Ada.Command_Line; use Ada.Command_Line;
with DJH.Events_and_Errors; use DJH.Events_and_Errors;
with Linux_Signals;
with Data_Logger; use Data_Logger;

procedure Power_Logger is

   Start_Message : constant String := "Power_Logger 20260317 Started """ &
     Argument (1) & """ """ & Argument (2) & """ """ & Argument (4) & '"';
   --  Password not logged!

begin -- Power_Logger
   delay 45.0; -- Wait for DNS service availability
   if Argument_Count /= 4 then
      raise Program_Error with
         "Missing some of Broker_Name, User_Name, Password or Topic";
   end if; -- Argument_Count /= 4
   Linux_Signals.Handlers.Install;
   Logger.Start (Argument (1), Argument (2), Argument (3), Argument (4));
   Put_Event (Start_Message);
   loop -- check for termination once per second
      delay 1.0;
      exit when Linux_Signals.Handlers.Signal_Stop or else
        Linux_Signals.Ctrl_C_Stop;
   end loop; -- check for termination once per second
   Linux_Signals.Handlers.Remove;
   Logger.Stop;
   Put_Event ("Normal exit");
   Stop_Events;
exception
   when E : others =>
      Put_Error ("Unhandled error", E);
      Put_Event ("Aborting Logger task");
      abort Logger;
      Put_Event ("Removing signal handlers");
      Linux_Signals.Handlers.Remove;
      Put_Event ("Error exit");
      Stop_Events;
end Power_Logger;