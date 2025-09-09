Wir müssen nun prüfen:

- Backups müssen für S3 & Local grundlegend identisch funktionieren
* Backups müssen einen wirklich echten progress via Event übermitteln (Bei S3 müssen wir die uploaded parts etc. einbeziehen), bei Local ist das ganze einfacher und ist auch bereits zum großteil implementiert, jedoch muss das einmal richtig getestet werden.
- Wenn ein Backup gestartet wird, muss der jeweils richtige State dafür gesetzt werden. Also Backup/Restore wie es bisher getan wird, aber es noch nicht richtig gemacht wird, da es durch andere dinge corrupted / überschrieben wird oder auch mal garnicht gesetzt wird. Auch der Reset muss richtig funktionieren, also entweder nehmen wir den vorherigen State oder wir ermitteln den state. Ich denke bei Backup Create nehmen wir den vorherigen State und bei Restore nehmen wir den echten state, also abfragen ob running oder offline oder wie auch immer das richtig heißt.
- Backups könnten failen, das bedeutet bspw. dass die Checksum nicht identisch ist, connection weg flog oder oder oder.. Das heißt dass wir einen retry definieren, wie oft ein backup versucht wird zu machen. Default sagen wir 2 mal und danach soll er das aufgeben und auch ein Event schicken dass ein backup nicht funktioniert hat. Also als Activity Event oder so, dass man das saved hat.
- Wir müssen es schaffen, dass die Backups nicht zu lange brauchen, dennoch muss das ganze maximal verständlich bleiben und wirklich sicher
- Die Formate wie gzip, zstd sollen später weiter ausbaubar sein - sprich dass sollte eine richtige Struktur haben und wartbar sein
- Schreibe für die kritischen dinge bei Backup tests, jedoch ohne die wings dabei zu testen, sondern wirklich nur die funktionalität der einzelnen services (Also Backup Create/Restore usw. mit richtigen Datein)
- Bei den S3 Backup und den fortschritt sollten wir das ganze bewusst 80/20 machen - wir bewerten hierbei die Backup Creation mit 80% und den Upload mit 20% - Damit der Percentage wirklich echt rüberkommt, sprich die restlichen 20% werden anhand von dem upload bei S3 ausgemacht. Bei Local ist dies natürlich nicht nötig und wird dann 100% von Create bewertet.

* Das ganze läuft in Production, muss also backwards compatibility sein.
* Vieles ist bereits in diese Richtung implementiert, aber dennoch noch nicht 100% funktionsfähig, daher agierst du als Senior-Go Experte und prüft die Logik mit deinen Agents im ersten Step, starte dafür mehrere Agents welche sich mehrere Files vornehmen und den zusammenhang / zusammenspiel technisch versuchen zu verstehen, erstelle dann einen plan das ganze prod ready zu optimieren oder ggf. neu zu entwerfen. Das ganze ist gewünscht minimal invasiv zu lösen, sofern es möglich ist! 

Mit dem Backup System ist gefordert, dass wir 100% Integrität dem Nutzer garantieren, Schnelligkeit aber auch einen reibungslosen ablauf der Software (Go) bereitstellen wollen, daher dürfen sich da keine Bugs einschleichen. 

Prüfe zudem, dass durch unsere Änderungen nichts an der Transfer Logik kaputt geht. 

Arbeite stets nach best practices, halte dich an die Source Struktur.